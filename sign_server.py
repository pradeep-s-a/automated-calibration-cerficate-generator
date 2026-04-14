"""
PDF Signing Server  —  with Authentication
===========================================
A lightweight Flask server that:
  - Serves the calibration certificate HTML at  GET  /
  - Signs uploaded PDFs at                      POST /sign      (🔒 auth required)
  - Caches calibration data at                  POST /calib-data (🔒 auth required)
  - Returns cached calibration data at          GET  /calib-data (open — same host)

Authentication
--------------
  The /sign and POST /calib-data endpoints require HTTP Basic Auth.
  Username : the signer name / key folder name (e.g. "alice")
  Password : that user's key passphrase

  The private key is always stored AES-256 encrypted.  The server NEVER
  decrypts it on startup — the passphrase is supplied per-request and
  discarded immediately after signing.  The server never stores the passphrase
  in memory between requests.

Usage:
  1. Generate keys (one-time):
         python pdf_signer.py keygen "Your Name" "Your Organisation"

  2. Start the server:
         python sign_server.py

  3. Open browser at  http://localhost:5000

  4. Fill in the certificate form, enter your passphrase in Step 6, click
     "Generate & Sign PDF".  The browser downloads the signed PDF.

  5. Verify a signed PDF:
         python pdf_signer.py verify  CalibCert_xxx.pdf
"""

import io
import json
import base64
import hashlib
import datetime
import functools
import re
from pathlib import Path
from flask import Flask, request, send_file, send_from_directory, jsonify, Response

# ── Signing logic ─────────────────────────────────────────────────────────────
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.utils import Prehashed
from cryptography.hazmat.backends import default_backend
from cryptography import x509
from cryptography.x509.oid import NameOID
from pypdf import PdfReader, PdfWriter

KEY_DIR  = Path("keys")
PRIV_KEY = KEY_DIR / "private_key.pem"
PUB_CERT = KEY_DIR / "certificate.pem"
PASSPHRASE_H = KEY_DIR / "passphrase.sha256"   # SHA-256 of key passphrase

HTML_FILE = Path(r"C:\Users\acer\Downloads\files1\calibration_certificate_generator_hart__6___2_.html")

app = Flask(__name__)


@app.after_request
def add_cors_headers(response: Response) -> Response:
    """Allow browser requests from file:// or another localhost origin."""
    origin = request.headers.get("Origin")
    response.headers["Access-Control-Allow-Origin"] = origin or "*"
    response.headers["Vary"] = "Origin"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    return response

# ── In-memory store for the latest calibration data ──────────────────────────
_latest_calib: dict | None = None


# ══════════════════════════════════════════════════════════════════════════════
# Key / cert helpers
# ══════════════════════════════════════════════════════════════════════════════

def _sanitize_username(username: str | None) -> str:
    name = (username or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
        return ""
    return name


def _key_paths_for_dir(key_dir: Path) -> dict[str, Path]:
    return {
        "dir": key_dir,
        "private_key": key_dir / "private_key.pem",
        "certificate": key_dir / "certificate.pem",
        "passphrase_hash": key_dir / "passphrase.sha256",
    }


def _paths_ready(paths: dict[str, Path]) -> bool:
    return paths["private_key"].exists() and paths["certificate"].exists()


def _user_key_dirs() -> list[Path]:
    if not KEY_DIR.exists():
        return []
    return sorted(
        child for child in KEY_DIR.iterdir()
        if child.is_dir() and _paths_ready(_key_paths_for_dir(child))
    )


def _resolve_key_paths(username: str | None = None) -> dict[str, Path] | None:
    clean_username = _sanitize_username(username)
    if not clean_username:
        return None
    user_paths = _key_paths_for_dir(KEY_DIR / clean_username)
    if _paths_ready(user_paths):
        return user_paths
    return None


def _keys_exist(username: str | None = None) -> bool:
    return _resolve_key_paths(username) is not None


def _verify_passphrase(paths: dict[str, Path], pw: str) -> bool:
    """Quick pre-check against stored hash before attempting key decryption."""
    if not paths["passphrase_hash"].exists():
        # No hash file — key might be unencrypted; let decryption decide
        return True
    expected = paths["passphrase_hash"].read_text().strip()
    return hashlib.sha256(pw.encode("utf-8")).hexdigest() == expected


def _load_key_with_passphrase(paths: dict[str, Path], pw: str):
    """Decrypt and return the private key, or raise ValueError on bad passphrase."""
    pw_bytes = pw.encode("utf-8") if pw else None
    try:
        return serialization.load_pem_private_key(
            paths["private_key"].read_bytes(), password=pw_bytes, backend=default_backend()
        )
    except (ValueError, TypeError) as exc:
        raise ValueError("Incorrect passphrase or corrupt key") from exc


def _load_certificate(paths: dict[str, Path]):
    return x509.load_pem_x509_certificate(paths["certificate"].read_bytes(), default_backend())


def _round_float(value, digits=4) -> float | None:
    try:
        return round(float(value), int(digits))
    except (TypeError, ValueError):
        return None


def _expected_ma_from_percent(setpoint_pct: float) -> float:
    return 4.0 + (16.0 * float(setpoint_pct) / 100.0)


def _expected_ma_from_temp(measured_temp: float, lrv: float, urv: float) -> float:
    span_t = float(urv) - float(lrv)
    if abs(span_t) < 1e-9:
        return 4.0
    return 4.0 + (((float(measured_temp) - float(lrv)) / span_t) * 16.0)


def _extract_float(value, digits=4) -> float | None:
    if isinstance(value, (int, float)):
        return _round_float(value, digits)
    if value in (None, ""):
        return None
    match = re.search(r"[-+]?\d+(?:\.\d+)?", str(value))
    if not match:
        return None
    return _round_float(match.group(0), digits)


def _range_limits(range_text) -> tuple[float, float]:
    numbers = re.findall(r"[-+]?\d+(?:\.\d+)?", str(range_text or ""))
    if len(numbers) >= 2:
        try:
            return float(numbers[0]), float(numbers[1])
        except ValueError:
            pass
    return 0.0, 100.0


def _format_signed(value: float, digits: int, suffix: str = "") -> str:
    sign = "+" if float(value) >= 0 else ""
    return f"{sign}{float(value):.{digits}f}{suffix}"


def _derive_cycle(index: int, points: list[float], current_cycle: str) -> str:
    prev_point = points[index - 1] if index > 0 else None
    next_point = points[index + 1] if index + 1 < len(points) else None
    this_point = points[index]
    if prev_point is None:
        return current_cycle
    if this_point < prev_point:
        return "Descending"
    if next_point is not None and this_point == prev_point and next_point < this_point:
        return "Descending"
    return current_cycle


def _enrich_result_payload(data: dict) -> dict:
    enriched = dict(data)
    rows_in = list(enriched.get("result_rows") or [])
    tolerance = _extract_float(enriched.get("tolerance"), 4)
    if tolerance is None and rows_in:
        tolerance = _extract_float(rows_in[0].get("tolerance_ma"), 4)
    if tolerance is None:
        tolerance = 0.080

    lrv, urv = _range_limits(enriched.get("iut_range"))
    span_t = urv - lrv
    calc_rows = []
    result_rows = []
    point_values = []

    for row in rows_in:
        setpoint_pct = _extract_float(row.get("setpoint_pct"), 3)
        set_temp = _extract_float(row.get("set_temp"), 3)
        if set_temp is None:
            set_temp = _extract_float(row.get("nominal_temp"), 3)
        if setpoint_pct is None:
            setpoint_pct = _extract_float(row.get("test_pt"), 3)
        if setpoint_pct is None and set_temp is not None and span_t:
            setpoint_pct = ((set_temp - lrv) / span_t) * 100.0
        point_values.append(float(setpoint_pct if setpoint_pct is not None else set_temp or 0.0))

    current_cycle = "Ascending"
    for index, row in enumerate(rows_in):
        row_index = int(_extract_float(row.get("row"), 0) or index)
        setpoint_pct = _extract_float(row.get("setpoint_pct"), 3)
        set_temp = _extract_float(row.get("set_temp"), 3)
        if set_temp is None:
            set_temp = _extract_float(row.get("nominal_temp"), 3)
        if setpoint_pct is None:
            setpoint_pct = _extract_float(row.get("test_pt"), 3)
        if setpoint_pct is None and set_temp is not None and span_t:
            setpoint_pct = ((set_temp - lrv) / span_t) * 100.0
        if set_temp is None and setpoint_pct is not None:
            set_temp = lrv + (setpoint_pct / 100.0) * span_t

        measured_temp = _extract_float(row.get("measured_temp"), 3)
        if measured_temp is None:
            measured_temp = _extract_float(row.get("applied_temp"), 3)

        measured_ma = _extract_float(row.get("measured_ma_value"), 4)
        if measured_ma is None:
            measured_ma = _extract_float(row.get("iut_ma"), 4)

        expected_ma = None
        if measured_temp is not None:
            expected_ma = round(_expected_ma_from_temp(measured_temp, lrv, urv), 4)
        elif setpoint_pct is not None:
            expected_ma = round(_expected_ma_from_percent(setpoint_pct), 4)
        elif set_temp is not None and span_t:
            expected_ma = round(_expected_ma_from_temp(set_temp, lrv, urv), 4)
        if expected_ma is None:
            expected_ma = _extract_float(row.get("expected_ma_value"), 4)
        if expected_ma is None:
            expected_ma = _extract_float(row.get("expected_ma"), 4)

        error_ma = None
        if measured_ma is not None and expected_ma is not None:
            error_ma = round(measured_ma - expected_ma, 4)
        if error_ma is None:
            error_ma = _extract_float(row.get("error_ma_value"), 4)
        if error_ma is None:
            error_ma = _extract_float(row.get("error_ma"), 4)

        pct_span_err = None
        if error_ma is not None:
            pct_span_err = round((error_ma / 16.0) * 100.0, 4)
        if pct_span_err is None:
            pct_span_err = _extract_float(row.get("pct_span_err_value"), 4)
        if pct_span_err is None:
            pct_span_err = _extract_float(row.get("pct_span_err"), 4)

        temp_error = _extract_float(row.get("temp_error"), 3)
        if temp_error is None and measured_temp is not None and set_temp is not None:
            temp_error = round(measured_temp - set_temp, 3)

        current_cycle = _derive_cycle(index, point_values, current_cycle)
        cycle = current_cycle
        status = str(row.get("status") or ("PASS" if abs(error_ma or 0.0) <= tolerance else "FAIL")).upper()

        calc_rows.append({
            "row": row_index,
            "cycle": cycle,
            "set_temp": round(set_temp or 0.0, 3),
            "theor_ma": round(expected_ma or 0.0, 4),
            "theor_pct": round(setpoint_pct or 0.0, 3),
            "meas_ma": round(measured_ma or 0.0, 4),
            "meas_pv": round(measured_temp or 0.0, 3),
            "ma_err": round(error_ma or 0.0, 4),
            "pct_span_err": round(pct_span_err or 0.0, 4),
            "temp_err": round(temp_error or 0.0, 3),
            "status": status,
        })
        result_rows.append({
            **row,
            "row": row_index,
            "cycle": cycle,
            "test_pt": f"{(setpoint_pct or 0.0):.1f} %",
            "nominal_temp": f"{(set_temp or 0.0):.2f} °C",
            "applied_temp": f"{(measured_temp or 0.0):.3f} °C",
            "iut_ma": f"{(measured_ma or 0.0):.4f}",
            "expected_ma": f"{(expected_ma or 0.0):.4f}",
            "error_ma": _format_signed(error_ma or 0.0, 4),
            "pct_span_err": _format_signed(pct_span_err or 0.0, 4, " %"),
            "tolerance_ma": f"±{tolerance:.3f}",
            "status": status,
            "setpoint_pct": round(setpoint_pct or 0.0, 3),
            "set_temp": round(set_temp or 0.0, 3),
            "measured_temp": round(measured_temp or 0.0, 3),
            "temp_error": round(temp_error or 0.0, 3),
            "measured_ma_value": round(measured_ma or 0.0, 4),
            "expected_ma_value": round(expected_ma or 0.0, 4),
            "error_ma_value": round(error_ma or 0.0, 4),
            "pct_span_err_value": round(pct_span_err or 0.0, 4),
        })

    pass_count = sum(1 for row in result_rows if row.get("status") == "PASS")
    total = len(result_rows)
    summary = f"{'PASS' if total and pass_count == total else 'FAIL'} - {pass_count}/{total} points within tolerance."
    max_abs_error_ma = max((abs(row["ma_err"]) for row in calc_rows), default=0.0)
    max_abs_pct_span_err = max((abs(row["pct_span_err"]) for row in calc_rows), default=0.0)
    max_abs_temp_error = max((abs(row["temp_err"]) for row in calc_rows), default=0.0)

    enriched["tolerance"] = tolerance
    enriched["result_rows"] = result_rows
    enriched["result"] = {
        "status": "PASS" if total and pass_count == total else "FAIL",
        "pass_count": pass_count,
        "total_points": total,
        "summary": summary,
        "max_abs_error_ma": round(max_abs_error_ma, 4),
        "max_abs_pct_span_err": round(max_abs_pct_span_err, 4),
        "max_abs_temp_error": round(max_abs_temp_error, 3),
        "rows": calc_rows,
    }
    enriched["result_summary"] = summary
    if not enriched.get("remarks"):
        enriched["remarks"] = (
            "Expected mA, mA error, % span error, and temperature error "
            "were derived automatically from the posted calibration data."
        )
    return enriched


def _looks_like_lora_session(data: dict) -> bool:
    rows = data.get("rows")
    return isinstance(rows, list) and bool(rows) and all(isinstance(row, dict) for row in rows)


def _normalize_lora_session(data: dict, source_name: str = "") -> dict:
    rows = data.get("rows") or []
    tolerance = 0.080
    result_rows = []

    for row in rows:
        if row.get("t") in {"text-row", "hex-row"}:
            setpoint_pct = _round_float(row.get("setpoint_pct"), 3) or 0.0
            measured_ma = _round_float(row.get("measured_ma"), 4) or 0.0
            measured_temp = _round_float(row.get("measured_temp"), 3) or 0.0
            expected_ma = round(_expected_ma_from_percent(setpoint_pct), 4)
            error_ma = round(measured_ma - expected_ma, 4)
            pct_span_err = round((error_ma / 16.0) * 100.0, 4)
            status = "PASS" if abs(error_ma) <= tolerance else "FAIL"
            sign = "+" if error_ma >= 0 else ""
            pct_sign = "+" if pct_span_err >= 0 else ""
            result_rows.append({
                "cycle": "Imported",
                "test_pt": f"{setpoint_pct:.0f} %",
                "nominal_temp": f"{setpoint_pct:.1f} % span",
                "applied_temp": f"{measured_temp:.3f} °C",
                "iut_ma": f"{measured_ma:.4f}",
                "expected_ma": f"{expected_ma:.4f}",
                "error_ma": f"{sign}{error_ma:.4f}",
                "pct_span_err": f"{pct_sign}{pct_span_err:.4f} %",
                "tolerance_ma": f"+/-{tolerance:.3f}",
                "status": status,
            })
            continue

        set_temp = _round_float(row.get("st") if "st" in row else row.get("set_temp"), 3)
        measured_ma = _round_float(row.get("mm") if "mm" in row else row.get("measured_ma"), 4)
        measured_temp = _round_float(row.get("pv") if "pv" in row else row.get("measured_temp"), 3)
        if set_temp is None or measured_ma is None or measured_temp is None:
            continue

        # Without full range metadata, interpret set_temp as a 0..100 span point when importing ad-hoc rows.
        expected_ma = round(_expected_ma_from_percent(set_temp), 4)
        error_ma = round(measured_ma - expected_ma, 4)
        pct_span_err = round((error_ma / 16.0) * 100.0, 4)
        status = "PASS" if abs(error_ma) <= tolerance else "FAIL"
        sign = "+" if error_ma >= 0 else ""
        pct_sign = "+" if pct_span_err >= 0 else ""
        result_rows.append({
            "cycle": str(row.get("cy") or row.get("cycle") or "Imported"),
            "test_pt": f"{set_temp:.0f} %",
            "nominal_temp": f"{set_temp:.3f} % span",
            "applied_temp": f"{measured_temp:.3f} °C",
            "iut_ma": f"{measured_ma:.4f}",
            "expected_ma": f"{expected_ma:.4f}",
            "error_ma": f"{sign}{error_ma:.4f}",
            "pct_span_err": f"{pct_sign}{pct_span_err:.4f} %",
            "tolerance_ma": f"+/-{tolerance:.3f}",
            "status": status,
        })

    pass_count = sum(1 for row in result_rows if row["status"] == "PASS")
    total = len(result_rows)
    summary_state = "PASS" if total and pass_count == total else "FAIL"
    source_label = source_name or data.get("session_id") or "LoRa session"
    started_at = str(data.get("started_at") or "")[:10]
    today = datetime.date.today().isoformat()

    return {
        "iut_desc": "Temperature Transmitter (LoRa Import)",
        "iut_mfr": "",
        "iut_model": "",
        "iut_serial": "",
        "iut_tag": data.get("session_id", ""),
        "iut_output": "4-20 mA DC / HART",
        "iut_range": "0 - 100 % span (LoRa imported session)",
        "iut_accuracy": "",
        "iut_supply": "24 V DC (external)",
        "iut_condition": "Imported from LoRa session data",
        "cal_date": started_at or today,
        "cert_date": today,
        "valid_until": "",
        "amb_temp": "",
        "cust_name": "",
        "tolerance": tolerance,
        "result_rows": result_rows,
        "result_summary": f"{summary_state} - {pass_count}/{total} points within tolerance.",
        "remarks": (
            f"Auto-generated from LoRa session data ({source_label}). "
            "Expected mA, errors, and PASS/FAIL were derived from the imported setpoint data."
        ),
        "source_session_id": data.get("session_id", ""),
        "source_format": data.get("meta", {}).get("format", "unknown"),
    }


def _normalize_calibration_payload(data: dict, source_name: str = "") -> dict:
    if not isinstance(data, dict):
        raise ValueError("Calibration payload must be a JSON object.")
    if "result_rows" in data:
        return _enrich_result_payload(data)
    if _looks_like_lora_session(data):
        return _enrich_result_payload(_normalize_lora_session(data, source_name=source_name))
    return data


# ══════════════════════════════════════════════════════════════════════════════
# HTTP Basic Auth decorator
# ══════════════════════════════════════════════════════════════════════════════

def _unauthorized(reason: str = "Authentication required") -> Response:
    return Response(
        json.dumps({"error": reason}),
        401,
        {
            "WWW-Authenticate": 'Basic realm="HART Sign Server"',
            "Content-Type": "application/json",
        },
    )


def require_auth(f):
    """Decorator: enforce HTTP Basic Auth.  Password must match key passphrase."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not auth.username or auth.password is None:
            return _unauthorized("HTTP Basic Auth required. "
                                 "Username: key owner / user folder name. Password: that user's signing passphrase.")

        key_paths = _resolve_key_paths(auth.username)
        if not key_paths:
            clean_username = _sanitize_username(auth.username)
            if _user_key_dirs():
                return jsonify({"error": f"No signing key configured for user '{clean_username or auth.username}'."}), 503
            return jsonify({"error": "Keys not found on server."}), 503

        if not _verify_passphrase(key_paths, auth.password):
            # Wrong passphrase — reject immediately without expensive key decrypt
            return _unauthorized("Incorrect passphrase.")

        return f(*args, key_paths=key_paths, **kwargs)
    return decorated


# ══════════════════════════════════════════════════════════════════════════════
# Core: sign PDF bytes in memory
# ══════════════════════════════════════════════════════════════════════════════

def sign_pdf_bytes(pdf_bytes: bytes, filename: str, passphrase: str, key_paths: dict[str, Path]) -> bytes:
    """
    Takes raw PDF bytes and a plaintext passphrase, decrypts the private key,
    signs the PDF, and returns signed PDF bytes.  No files written to disk.
    """
    private_key = _load_key_with_passphrase(key_paths, passphrase)   # raises on bad pw
    cert        = _load_certificate(key_paths)

    timestamp   = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    cert_serial = hex(cert.serial_number)
    subject_cn  = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value

    meta_dict = {
        "/DigitallySigned": "true",
        "/SignerName":      subject_cn,
        "/SignatureDate":   timestamp,
    }

    # 1. Base PDF (no attachment yet)
    base_buf = io.BytesIO()
    reader   = PdfReader(io.BytesIO(pdf_bytes))
    writer   = PdfWriter()
    for page in reader.pages:
        writer.add_page(page)
    writer.add_metadata(meta_dict)
    writer.write(base_buf)
    base_bytes = base_buf.getvalue()

    # 2. Hash
    digest     = hashlib.sha256(base_bytes).digest()
    hex_digest = digest.hex()

    # 3. Sign
    signature_bytes = private_key.sign(
        digest, padding.PKCS1v15(), Prehashed(hashes.SHA256())
    )
    b64_sig = base64.b64encode(signature_bytes).decode()

    # 4. Embed signature attachment
    sig_meta = {
        "version":     "1.0",
        "algorithm":   "RSA-PKCS1v15-SHA256",
        "signed_file": filename,
        "sha256":      hex_digest,
        "timestamp":   timestamp,
        "signer":      subject_cn,
        "cert_serial": cert_serial,
        "certificate": key_paths["certificate"].read_text(),
        "signature":   b64_sig,
    }
    sig_json = json.dumps(sig_meta, indent=2).encode()

    reader2 = PdfReader(io.BytesIO(base_bytes))
    writer2  = PdfWriter()
    for page in reader2.pages:
        writer2.add_page(page)
    writer2.add_metadata(meta_dict)
    writer2.add_attachment("signature.sig", sig_json)

    out_buf = io.BytesIO()
    writer2.write(out_buf)
    return out_buf.getvalue()


# ══════════════════════════════════════════════════════════════════════════════
# Routes
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    """Serve the calibration certificate HTML."""
    if not HTML_FILE.exists():
        return (
            "HTML file not found. Place the certificate HTML in the same folder as sign_server.py.",
            404,
        )
    response = send_from_directory(".", HTML_FILE.name)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/status")
def status():
    """Health-check / key status (no auth — used by the HTML badge check)."""
    username = request.args.get("username", "").strip()
    key_paths = _resolve_key_paths(username or None)
    user_dirs = _user_key_dirs()
    if not key_paths:
        error = "Keys not found. Run: python pdf_signer.py keygen"
        if username and user_dirs:
            error = f"No signing key configured for user '{username}'."
        return jsonify({
            "ready": False,
            "error": error,
            "auth": True,
        }), 503

    cert = _load_certificate(key_paths)
    cn = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
    enc_key = key_paths["passphrase_hash"].exists()
    signer_label = cn
    if user_dirs and key_paths["dir"] == KEY_DIR:
        signer_label = f"{cn} (shared default)"

    return jsonify({
        "ready": True,
        "signer": signer_label,
        "cert_valid": str(cert.not_valid_after_utc.date()),
        "auth_required": True,
        "key_encrypted": enc_key,
        "multi_user": bool(user_dirs),
        "users": [p.name for p in user_dirs],
    })


# ── Calibration data ─────────────────────────────────────────────────────────

@app.route("/calib-data", methods=["POST"])
@require_auth
def calib_data_post(key_paths=None):
    """
    🔒 Auth required.
    Receive calibration data from the HART Communicator desktop app and cache it.
    """
    global _latest_calib
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 400
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Empty or invalid JSON body"}), 400
    source_name = ""
    if isinstance(data, dict) and data.get("file_path"):
        candidate = Path(str(data.get("file_path"))).expanduser()
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        if not candidate.exists():
            return jsonify({"error": f"Calibration file not found: {candidate}"}), 404
        try:
            data = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception as exc:
            return jsonify({"error": f"Could not read calibration file: {exc}"}), 400
        source_name = candidate.name

    _latest_calib = _normalize_calibration_payload(data, source_name=source_name)
    rows = len(_latest_calib.get("result_rows", []))
    rng  = _latest_calib.get("iut_range", "?")
    print(f"[calib-data] ✓ Received from {request.authorization.username!r} — "
          f"{rows} rows, range {rng}")
    return jsonify({"status": "ok", "rows": rows}), 200


@app.route("/calib-data", methods=["GET"])
def calib_data_get():
    """
    Return the latest cached calibration data.
    Open (no auth) — the HTML page fetches this from the same localhost origin.
    Returns 204 No Content if nothing has been posted yet.
    """
    if _latest_calib is None:
        response = Response("", 204)
    else:
        response = jsonify(_latest_calib)
        response.status_code = 200
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# ── PDF signing ───────────────────────────────────────────────────────────────

@app.route("/sign", methods=["POST"])
@require_auth
def sign(key_paths=None):
    """
    🔒 Auth required (HTTP Basic Auth — password = signing passphrase).
    Receive a PDF, sign it, return the signed PDF.
    """
    # Grab passphrase from Basic Auth credentials (already verified by decorator)
    passphrase = request.authorization.password

    # Accept multipart upload or raw binary body
    if request.files and "file" in request.files:
        f         = request.files["file"]
        pdf_bytes = f.read()
        filename  = f.filename or "signed.pdf"
    else:
        pdf_bytes = request.data
        filename  = request.args.get("filename", "signed.pdf")

    if not pdf_bytes:
        return jsonify({"error": "No PDF data received."}), 400

    try:
        signed_bytes = sign_pdf_bytes(pdf_bytes, filename, passphrase, key_paths)
    except ValueError as e:
        # Bad passphrase reaching this far (hash check passed but key decrypt failed)
        return jsonify({"error": str(e)}), 403
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    username = request.authorization.username
    print(f"[sign] ✓ Signed '{filename}' for user '{username}'")

    return send_file(
        io.BytesIO(signed_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=filename,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print()
    print("=" * 58)
    print("  HART PDF Signing Server")
    print("=" * 58)

    user_dirs = _user_key_dirs()
    default_paths = _resolve_key_paths()

    if not default_paths and not user_dirs:
        print()
        print("⚠️   Keys not found.")
        print("    Run first:  python pdf_signer.py keygen \"Your Name\" \"Your Org\"")
    else:
        if user_dirs:
            print(f"ðŸ”‘  User keys     : {', '.join(p.name for p in user_dirs)}")
        chosen_paths = default_paths if default_paths else _key_paths_for_dir(user_dirs[0])
        cert = _load_certificate(chosen_paths)
        cn   = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
        enc  = chosen_paths["passphrase_hash"].exists()
        print(f"🔑  Signing key  : {cn}")
        print(f"🔒  Key encrypted: {'Yes — passphrase required per request' if enc else 'No  ⚠ (run keygen to add one)'}")
        print(f"📄  Cert expires : {cert.not_valid_after_utc.date()}")

    print()
    print("🔐  Authentication : HTTP Basic Auth")
    print("    Username       : key owner / user folder name")
    print("    Password       : that user's signing key passphrase")
    print()
    print("🚀  Server         : http://localhost:5000")
    print()
    app.run(host="0.0.0.0", port=5000, debug=False)
