#!/usr/bin/env python3
"""
Receive LoRa calibration packets on a PC through an ESP32 USB serial link.

Expected ESP32 serial line format:
    LORA_JSON {"payload":"{...}","rssi":-90,"snr":7.5,"len":123}

Usage:
    python pc_lora_receiver.py --port COM5
"""

import argparse
import base64
import csv
import json
import os
import struct
import subprocess
import time
import urllib.error
import urllib.request
import webbrowser
from datetime import datetime

HEX_END_SIGNAL = "ff0000000000000000"
TEXT_END_SIGNAL = "99999999999"
TEXT_END_REPEAT_COUNT = 3
SETPOINT_CODE_TO_PERCENT = {
    0: 0.0,
    1: 25.0,
    2: 50.0,
    3: 75.0,
    4: 100.0,
}


class SessionCollector:
    def __init__(self, out_dir):
        self.out_dir = out_dir
        self.sessions = {}
        self.start_fragments = {}
        self.row_fragments = {}
        self.seen_packet_keys = set()
        self.current_text_session_id = None
        self.text_session_counter = 0

    def _get_session(self, session_id):
        if session_id not in self.sessions:
            self.sessions[session_id] = {
                "session_id": session_id,
                "started_at": None,
                "meta": {},
                "rows": [],
                "hysteresis": [],
                "end": None,
                "single": None,
                "text_rows_map": {},
                "text_end_signals": 0,
                "gateway_packets": [],
            }
        return self.sessions[session_id]

    def _record_gateway_packet(self, session, wrapper, payload):
        session["gateway_packets"].append({
            "pc_received_at": datetime.now().isoformat(timespec="seconds"),
            "rssi": wrapper.get("rssi"),
            "snr": wrapper.get("snr"),
            "len": wrapper.get("len"),
            "payload": payload,
        })

    def _start_hex_session(self, wrapper):
        self.text_session_counter += 1
        session_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}{self.text_session_counter:02d}"
        session = self._get_session(session_id)
        session["started_at"] = wrapper.get("pc_received_at") or datetime.now().isoformat(timespec="seconds")
        session["meta"] = {
            "format": "hex9",
            "end_signal": HEX_END_SIGNAL,
            "end_repeat_count": TEXT_END_REPEAT_COUNT,
        }
        self.current_text_session_id = session_id
        return session_id

    def _start_text_session(self, wrapper):
        self.text_session_counter += 1
        session_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}{self.text_session_counter:02d}"
        session = self._get_session(session_id)
        session["started_at"] = wrapper.get("pc_received_at") or datetime.now().isoformat(timespec="seconds")
        session["meta"] = {
            "format": "text11",
            "end_signal": TEXT_END_SIGNAL,
            "end_repeat_count": TEXT_END_REPEAT_COUNT,
        }
        self.current_text_session_id = session_id
        return session_id

    def _parse_hex_row(self, payload_text):
        raw_bytes = bytes.fromhex(payload_text)
        if len(raw_bytes) != 9:
            raise ValueError(f"HEX payload must be exactly 9 bytes, got {len(raw_bytes)} bytes.")

        header = raw_bytes[0]
        cycle_code = (header >> 4) & 0x0F
        setpoint_code = header & 0x0F
        if cycle_code == 0:
            cycle = "Ascending"
            row_index = setpoint_code
        elif cycle_code == 1:
            cycle = "Descending"
            row_index = 9 - setpoint_code
        else:
            raise ValueError(f"Unsupported cycle nibble 0x{cycle_code:X} in HEX payload.")

        if setpoint_code not in SETPOINT_CODE_TO_PERCENT:
            raise ValueError(f"Unsupported setpoint code 0x{setpoint_code:X} in HEX payload.")

        measured_temp = struct.unpack(">f", raw_bytes[1:5])[0]
        measured_ma = struct.unpack(">f", raw_bytes[5:9])[0]
        return {
            "t": "hex-row",
            "row": row_index,
            "cycle": cycle,
            "setpoint_code": setpoint_code,
            "setpoint_pct": SETPOINT_CODE_TO_PERCENT[setpoint_code],
            "measured_ma": measured_ma,
            "measured_temp": measured_temp,
            "raw": payload_text.lower(),
        }

    def _parse_text11_row(self, payload_text):
        row_index = int(payload_text[0])
        setpoint_pct = int(payload_text[1:4])
        measured_ma = int(payload_text[4:7]) / 10.0
        measured_temp = int(payload_text[7:11]) / 10.0
        return {
            "t": "text-row",
            "row": row_index,
            "setpoint_pct": setpoint_pct,
            "measured_ma": measured_ma,
            "measured_temp": measured_temp,
            "raw": payload_text,
        }

    def _process_text_payload(self, wrapper, payload_text):
        payload_hex = "".join(str(payload_text).split()).lower()
        if payload_hex == HEX_END_SIGNAL:
            session_id = self.current_text_session_id or self._start_hex_session(wrapper)
            session = self._get_session(session_id)
            session["text_end_signals"] += 1
            payload = {
                "t": "hex-end",
                "signal": payload_hex,
                "count": session["text_end_signals"],
                "required": TEXT_END_REPEAT_COUNT,
                "complete": session["text_end_signals"] >= TEXT_END_REPEAT_COUNT,
            }
            if payload["complete"]:
                session["end"] = {
                    "t": "end",
                    "signal": payload_hex,
                    "count": session["text_end_signals"],
                    "total": len(session["text_rows_map"]),
                    "res": "COMPLETE",
                }
                self.current_text_session_id = None
            self._record_gateway_packet(session, wrapper, payload)
            return session_id, "hex-end", payload

        try:
            bytes.fromhex(payload_hex)
        except ValueError:
            payload_hex = None

        if payload_hex and len(payload_hex) == 18:
            session_id = self.current_text_session_id or self._start_hex_session(wrapper)
            session = self._get_session(session_id)
            session["text_end_signals"] = 0
            payload = self._parse_hex_row(payload_hex)
            session["text_rows_map"][payload["row"]] = payload
            self._record_gateway_packet(session, wrapper, payload)
            return session_id, "hex-row", payload

        if payload_text == TEXT_END_SIGNAL:
            session_id = self.current_text_session_id or self._start_text_session(wrapper)
            session = self._get_session(session_id)
            session["text_end_signals"] += 1
            payload = {
                "t": "text-end",
                "signal": payload_text,
                "count": session["text_end_signals"],
                "required": TEXT_END_REPEAT_COUNT,
                "complete": session["text_end_signals"] >= TEXT_END_REPEAT_COUNT,
            }
            if payload["complete"]:
                session["end"] = {
                    "t": "end",
                    "signal": payload_text,
                    "count": session["text_end_signals"],
                    "total": len(session["text_rows_map"]),
                    "res": "COMPLETE",
                }
                self.current_text_session_id = None
            self._record_gateway_packet(session, wrapper, payload)
            return session_id, "text-end", payload

        if len(payload_text) == 11 and payload_text.isdigit():
            session_id = self.current_text_session_id or self._start_text_session(wrapper)
            session = self._get_session(session_id)
            session["text_end_signals"] = 0
            payload = self._parse_text11_row(payload_text)
            session["text_rows_map"][payload["row"]] = payload
            self._record_gateway_packet(session, wrapper, payload)
            return session_id, "text-row", payload

        return None, "text", payload_text

    def _consume_start_fragment(self, payload):
        session_id = str(payload.get("id") or "")
        if not session_id:
            return None

        fragment = self.start_fragments.setdefault(session_id, {"id": session_id})
        if payload.get("t") == "s1":
            fragment["ts"] = payload.get("ts")
            fragment["tag"] = payload.get("tag")
            fragment["la"] = payload.get("la")
        else:
            fragment["lrv"] = payload.get("lrv")
            fragment["urv"] = payload.get("urv")
            fragment["tol"] = payload.get("tol")
            fragment["rows"] = payload.get("rows")
            fragment["hys"] = payload.get("hys")

        required = ("id", "ts", "tag", "la", "lrv", "urv", "tol", "rows", "hys")
        if not all(key in fragment for key in required):
            return None

        self.start_fragments.pop(session_id, None)
        return {
            "t": "start",
            "id": fragment["id"],
            "ts": fragment["ts"],
            "tag": fragment["tag"],
            "la": fragment["la"],
            "lrv": fragment["lrv"],
            "urv": fragment["urv"],
            "tol": fragment["tol"],
            "rows": fragment["rows"],
            "hys": fragment["hys"],
        }

    def _consume_row_fragment(self, payload):
        session_id = str(payload.get("id") or "")
        row_index = payload.get("i")
        if not session_id or row_index is None:
            return None

        key = (session_id, int(row_index))
        fragment = self.row_fragments.setdefault(key, {"id": session_id, "i": int(row_index)})
        if payload.get("t") == "r1":
            fragment["cy"] = payload.get("cy")
            fragment["st"] = payload.get("st")
            fragment["tm"] = payload.get("tm")
            fragment["mm"] = payload.get("mm")
        else:
            fragment["pv"] = payload.get("pv")
            fragment["me"] = payload.get("me")
            fragment["pe"] = payload.get("pe")
            fragment["te"] = payload.get("te")
            fragment["ok"] = payload.get("ok")

        required = ("id", "i", "cy", "st", "tm", "mm", "pv", "me", "pe", "te", "ok")
        if not all(key_name in fragment for key_name in required):
            return None

        self.row_fragments.pop(key, None)
        return {
            "t": "row",
            "id": fragment["id"],
            "i": fragment["i"],
            "cy": fragment["cy"],
            "st": fragment["st"],
            "tm": fragment["tm"],
            "mm": fragment["mm"],
            "pv": fragment["pv"],
            "me": fragment["me"],
            "pe": fragment["pe"],
            "te": fragment["te"],
            "ok": fragment["ok"],
        }

    def _maybe_reassemble_payload(self, payload):
        packet_type = payload.get("t")
        if packet_type in ("s1", "s2"):
            return self._consume_start_fragment(payload)
        if packet_type in ("r1", "r2"):
            return self._consume_row_fragment(payload)
        return payload

    def _is_duplicate_payload(self, payload):
        session_id = str(payload.get("id") or "")
        packet_key = payload.get("k")
        if not session_id or packet_key is None:
            return False

        dedupe_key = (session_id, int(packet_key))
        if dedupe_key in self.seen_packet_keys:
            return True

        self.seen_packet_keys.add(dedupe_key)
        return False

    def process_wrapper(self, wrapper):
        payload_text = wrapper.get("payload")
        if not isinstance(payload_text, str):
            raise ValueError("Gateway packet does not contain a string payload.")

        payload_text = payload_text.strip()
        if not payload_text:
            raise ValueError("Gateway packet payload is empty.")

        if not payload_text.startswith("{"):
            return self._process_text_payload(wrapper, payload_text)

        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid LoRa payload JSON: {exc}") from exc

        if payload.get("t") == "ack":
            return None, "ack", payload
        if self._is_duplicate_payload(payload):
            return None, "duplicate", payload

        payload = self._maybe_reassemble_payload(payload)
        if payload is None:
            return None, "fragment", None

        session_id = str(payload.get("id") or "")
        if not session_id:
            return None, "json", payload

        session = self._get_session(session_id)
        self._record_gateway_packet(session, wrapper, payload)

        packet_type = payload.get("t") or ("single" if "d" in payload else None)
        if packet_type == "start":
            session["started_at"] = payload.get("ts")
            session["meta"] = payload
        elif packet_type == "row":
            session["rows"].append(payload)
        elif packet_type == "hys":
            session["hysteresis"].append(payload)
        elif packet_type == "end":
            session["end"] = payload
        elif packet_type == "single":
            session["single"] = payload

        return session_id, packet_type, payload

    def save_session(self, session_id):
        session = self._get_session(session_id)
        if session["text_rows_map"]:
            rows = [session["text_rows_map"][index] for index in sorted(session["text_rows_map"])]
        else:
            rows = sorted(session["rows"], key=lambda item: item.get("i", 0))
        data = {
            "session_id": session["session_id"],
            "started_at": session["started_at"],
            "meta": session["meta"],
            "rows": rows,
            "hysteresis": sorted(session["hysteresis"], key=lambda item: item.get("i", 0)),
            "end": session["end"],
            "single": session["single"],
            "gateway_packets": session["gateway_packets"],
        }
        path = os.path.join(self.out_dir, f"calibration_{session_id}.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, ensure_ascii=False)
        return path

    def save_all(self):
        for session_id in list(self.sessions):
            self.save_session(session_id)


def _fmt_num(value, digits=3, unit=""):
    try:
        return f"{float(value):.{digits}f}{unit}"
    except (TypeError, ValueError):
        return f"?{unit}"


def _fmt_radio(wrapper):
    return f"RSSI={wrapper.get('rssi')}dBm SNR={wrapper.get('snr')}dB"


def _profile_csv_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "certificate_profiles.csv")


def load_certificate_profile(profile_tag):
    wanted = (profile_tag or "YTA610").strip().upper()
    csv_path = _profile_csv_path()
    if os.path.exists(csv_path):
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                tag_value = str(row.get("tag") or "").strip().upper()
                if tag_value == wanted:
                    return row
    return {
        "tag": "YTA610",
        "long_address": "B7 11 8B A2 28",
        "type": "temperature",
        "lrv": "0",
        "urv": "175",
        "tolerance": "0.080",
        "iut_desc": "Digital Temperature Transmitter",
        "iut_mfr": "Yokogawa Electric Corp.",
        "iut_model": "YTA610/SCL",
        "iut_serial": "",
        "iut_tag": "YTA610",
        "iut_range": "0 - 175 C (RTD PT100)",
        "iut_output": "4 - 20 mA DC / HART Rev.7",
        "iut_accuracy": "+/-0.1 % of Span (mfr. spec.)",
        "iut_supply": "24 V DC (external)",
        "iut_condition": "",
        "cust_name": "",
        "amb_temp": "23 C +/- 1 C",
    }


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _expected_ma_from_temp(measured_temp, lrv, urv):
    span_t = float(urv) - float(lrv)
    if abs(span_t) < 1e-9:
        return 4.0
    return 4.0 + (((float(measured_temp) - float(lrv)) / span_t) * 16.0)


def _profile_value(profile, *names, default=""):
    for name in names:
        value = profile.get(name)
        if value not in (None, ""):
            return value
    return default


def build_hart_payload_from_session(session_data, profile_tag):
    profile = load_certificate_profile(profile_tag)
    lrv = _safe_float(_profile_value(profile, "lrv", default=0.0), 0.0)
    urv = _safe_float(_profile_value(profile, "urv", default=175.0), 175.0)
    tol = _safe_float(_profile_value(profile, "tolerance", "tol_ma", "tolerance_ma", default=0.080), 0.080)
    rows_in = list(session_data.get("rows") or [])
    span_t = urv - lrv

    result_rows = []
    calc_rows = []
    setpoints = [_safe_float(row.get("setpoint_pct"), 0.0) for row in rows_in]
    current_cycle = "Ascending"

    for index, row in enumerate(rows_in):
        row_index = int(_safe_float(row.get("row"), len(calc_rows)))
        setpoint_pct = setpoints[index]
        measured_ma = _safe_float(row.get("measured_ma"), 0.0)
        measured_temp = _safe_float(row.get("measured_temp"), 0.0)
        set_temp = lrv + (setpoint_pct / 100.0) * span_t
        prev_pct = setpoints[index - 1] if index > 0 else None
        next_pct = setpoints[index + 1] if index + 1 < len(setpoints) else None
        if prev_pct is not None:
            if setpoint_pct < prev_pct:
                current_cycle = "Descending"
            elif next_pct is not None and setpoint_pct == prev_pct and next_pct < setpoint_pct:
                current_cycle = "Descending"

        theor_ma = _expected_ma_from_temp(measured_temp, lrv, urv)
        ma_err = measured_ma - theor_ma
        pct_span_err = (ma_err / 16.0) * 100.0
        temp_err = measured_temp - set_temp
        status = "PASS" if abs(ma_err) <= tol else "FAIL"
        sign = "+" if ma_err >= 0 else ""
        pct_sign = "+" if pct_span_err >= 0 else ""

        calc_row = {
            "row": row_index,
            "cycle": current_cycle,
            "set_temp": set_temp,
            "theor_ma": theor_ma,
            "theor_pct": setpoint_pct,
            "meas_ma": measured_ma,
            "meas_pv": measured_temp,
            "ma_err": ma_err,
            "pct_span_err": pct_span_err,
            "temp_err": temp_err,
            "status": status,
        }
        calc_rows.append(calc_row)
        result_rows.append({
            "row": row_index,
            "cycle": current_cycle,
            "test_pt": f"{setpoint_pct:.1f} %",
            "nominal_temp": f"{set_temp:.2f} °C",
            "applied_temp": f"{measured_temp:.3f} °C",
            "iut_ma": f"{measured_ma:.4f}",
            "expected_ma": f"{theor_ma:.4f}",
            "error_ma": f"{sign}{ma_err:.4f}",
            "pct_span_err": f"{pct_sign}{pct_span_err:.4f} %",
            "tolerance_ma": f"±{tol:.3f}",
            "status": status,
            "setpoint_pct": round(setpoint_pct, 3),
            "set_temp": round(set_temp, 3),
            "measured_temp": round(measured_temp, 3),
            "temp_error": round(temp_err, 3),
            "measured_ma_value": round(measured_ma, 4),
            "expected_ma_value": round(theor_ma, 4),
            "error_ma_value": round(ma_err, 4),
            "pct_span_err_value": round(pct_span_err, 4),
        })

    asc_map = {round(row["set_temp"], 6): row for row in calc_rows if row["cycle"] == "Ascending"}
    desc_map = {round(row["set_temp"], 6): row for row in calc_rows if row["cycle"] == "Descending"}
    hyst_parts = []
    for key in sorted(set(asc_map) & set(desc_map)):
        asc = asc_map[key]
        desc = desc_map[key]
        hyst_ma = asc["ma_err"] - desc["ma_err"]
        hyst_pct = asc["pct_span_err"] - desc["pct_span_err"]
        hyst_parts.append(f"{asc['set_temp']:.1f}°C: {hyst_ma:+.4f} mA ({hyst_pct:+.3f} % span)")
    hyst_str = "  |  ".join(hyst_parts) if hyst_parts else "N/A"

    pass_count = sum(1 for row in result_rows if row["status"] == "PASS")
    total = len(result_rows)
    max_abs_error_ma = max((abs(row["ma_err"]) for row in calc_rows), default=0.0)
    max_abs_pct_span_err = max((abs(row["pct_span_err"]) for row in calc_rows), default=0.0)
    max_abs_temp_err = max((abs(row["temp_err"]) for row in calc_rows), default=0.0)
    result_summary = f"{'PASS' if pass_count == total and total > 0 else 'FAIL'} — {pass_count}/{total} points within tolerance."

    try:
        started = datetime.fromisoformat(str(session_data.get("started_at") or ""))
    except ValueError:
        started = datetime.now()
    today = started.strftime("%Y-%m-%d")
    valid_until = started.replace(year=started.year + 1).strftime("%Y-%m-%d")

    return {
        "type": _profile_value(profile, "type", "instrument_type", "device_type", default="temperature"),
        "iut_desc": _profile_value(profile, "iut_desc", "description", default="Digital Temperature Transmitter"),
        "iut_mfr": _profile_value(profile, "iut_mfr", "manufacturer", default="Yokogawa Electric Corp."),
        "iut_model": _profile_value(profile, "iut_model", "model", "model_no", "part_no", default="YTA610/SCL"),
        "iut_serial": _profile_value(profile, "iut_serial", "serial", "serial_no"),
        "iut_tag": _profile_value(profile, "iut_tag", "tag_no", "tag", default="YTA610"),
        "long_address": _profile_value(profile, "long_address", "long_addr", "hart_long_address"),
        "iut_output": _profile_value(profile, "iut_output", "output_signal", default="4 – 20 mA DC  /  HART Rev.7"),
        "iut_range": _profile_value(profile, "iut_range", "range", default=f"{lrv:.1f} – {urv:.1f} °C"),
        "iut_accuracy": _profile_value(profile, "iut_accuracy", "accuracy", default="±0.5 % of Span (mfr. spec.)"),
        "iut_supply": _profile_value(profile, "iut_supply", "supply_voltage", default="24 V DC (external)"),
        "iut_condition": _profile_value(profile, "iut_condition", "condition"),
        "cust_name": _profile_value(profile, "cust_name", "customer", "customer_name"),
        "cal_date": today,
        "cert_date": today,
        "valid_until": valid_until,
        "amb_temp": _profile_value(profile, "amb_temp", "ambient", "ambient_temp", default="23 °C ± 1 °C"),
        "tolerance": tol,
        "result_rows": result_rows,
        "result": {
            "status": "PASS" if pass_count == total and total > 0 else "FAIL",
            "pass_count": pass_count,
            "total_points": total,
            "summary": result_summary,
            "max_abs_error_ma": round(max_abs_error_ma, 4),
            "max_abs_pct_span_err": round(max_abs_pct_span_err, 4),
            "max_abs_temp_error": round(max_abs_temp_err, 3),
            "rows": calc_rows,
        },
        "hyst_summary": hyst_str,
        "result_summary": result_summary,
        "remarks": (
            f"Temperature transmitter calibrated over range {lrv:.1f}–{urv:.1f} °C "
            f"using imported LoRa session data. {result_summary}  "
            f"Hysteresis: {hyst_str}. "
            "Expected mA, mA error, % span error, and temperature error were derived from the received setpoint data."
        ),
    }


def post_session_to_sign_server(session_path, server_url, username, password, profile_tag):
    if not username or password is None:
        raise ValueError(
            "Signing server credentials are required. Provide --sign-username and "
            "--sign-password or set SIGN_SERVER_USERNAME and SIGN_SERVER_PASSWORD."
        )

    with open(session_path, "r", encoding="utf-8") as handle:
        session_data = json.load(handle)

    hart_data = build_hart_payload_from_session(session_data, profile_tag)
    payload = json.dumps(hart_data, ensure_ascii=False).encode("utf-8")
    endpoint = server_url.rstrip("/") + "/calib-data"
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    request_obj = urllib.request.Request(
        endpoint,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {token}",
        },
        method="POST",
    )
    with urllib.request.urlopen(request_obj, timeout=15) as response:
        body = response.read().decode("utf-8", errors="replace")
        return response.status, body


def launch_signing_browser(server_url):
    url = server_url.rstrip("/") + f"/?ts={int(time.time() * 1000)}"
    try:
        if webbrowser.open_new_tab(url):
            return url
    except Exception:
        pass

    try:
        if os.name == "nt" and hasattr(os, "startfile"):
            os.startfile(url)
            return url
        if os.name == "posix":
            opener = "open" if os.uname().sysname == "Darwin" else "xdg-open"
            subprocess.Popen([opener, url])
            return url
    except Exception:
        pass

    return None


def handle_completed_session(session_path, args):
    status, body = post_session_to_sign_server(
        session_path,
        args.sign_server_url,
        args.sign_username,
        args.sign_password,
        args.profile_tag,
    )
    print(f"Posted session to signing server: HTTP {status} {body}")
    browser_url = launch_signing_browser(args.sign_server_url)
    if browser_url:
        print(f"Opened signing page: {browser_url}")
    else:
        print("Could not open browser automatically. Open the signing page manually:", args.sign_server_url)


def format_hart_packet(wrapper, session_id, packet_type, payload):
    timestamp = wrapper["pc_received_at"]
    radio = _fmt_radio(wrapper)

    if packet_type == "start":
        tag = payload.get("tag") or "-"
        rows = payload.get("rows", "?")
        hys = payload.get("hys", "?")
        lrv = _fmt_num(payload.get("lrv"), 3, "C")
        urv = _fmt_num(payload.get("urv"), 3, "C")
        tol = _fmt_num(payload.get("tol"), 4, "mA")
        return (
            f"[{timestamp}] HART START id={session_id} tag={tag} "
            f"range={lrv}..{urv} tol={tol} rows={rows} hys={hys} {radio}"
        )

    if packet_type == "row":
        cycle = payload.get("cy", "?")
        index = int(payload.get("i", 0))
        status = "PASS" if payload.get("ok") else "FAIL"
        return (
            f"[{timestamp}] HART ROW {index:02d} {cycle} "
            f"set={_fmt_num(payload.get('st'), 3, 'C')} "
            f"theor={_fmt_num(payload.get('tm'), 4, 'mA')} "
            f"meas={_fmt_num(payload.get('mm'), 4, 'mA')} "
            f"err={_fmt_num(payload.get('me'), 4, 'mA')} "
            f"pv={_fmt_num(payload.get('pv'), 3, 'C')} "
            f"temp_err={_fmt_num(payload.get('te'), 4, 'C')} "
            f"{status} {radio}"
        )

    if packet_type == "hys":
        index = int(payload.get("i", 0))
        return (
            f"[{timestamp}] HART HYS {index:02d} "
            f"set={_fmt_num(payload.get('st'), 3, 'C')} "
            f"hyst={_fmt_num(payload.get('hm'), 4, 'mA')} "
            f"pct={_fmt_num(payload.get('hp'), 4, '%')} "
            f"temp={_fmt_num(payload.get('ht'), 4, 'C')} "
            f"{radio}"
        )

    if packet_type == "hex-row":
        return (
            f"[{timestamp}] HART HEX ROW {int(payload.get('row', 0)):02d} "
            f"{payload.get('cycle', '?')} "
            f"setpct={int(payload.get('setpoint_pct', 0)):03d} "
            f"measma={_fmt_num(payload.get('measured_ma'), 4, 'mA')} "
            f"meastemp={_fmt_num(payload.get('measured_temp'), 3, 'C')} "
            f"raw={payload.get('raw', '')} "
            f"{radio}"
        )

    if packet_type == "hex-end":
        return (
            f"[{timestamp}] HART HEX END "
            f"{int(payload.get('count', 0))}/{int(payload.get('required', 0))} "
            f"{radio}"
        )

    if packet_type == "text-row":
        return (
            f"[{timestamp}] HART TEXT ROW {int(payload.get('row', 0)):02d} "
            f"setpct={int(payload.get('setpoint_pct', 0)):03d} "
            f"measma={_fmt_num(payload.get('measured_ma'), 1, 'mA')} "
            f"meastemp={_fmt_num(payload.get('measured_temp'), 1, 'C')} "
            f"{radio}"
        )

    if packet_type == "text-end":
        return (
            f"[{timestamp}] HART TEXT END "
            f"{int(payload.get('count', 0))}/{int(payload.get('required', 0))} "
            f"{radio}"
        )

    if packet_type == "single":
        return f"[{timestamp}] HART SINGLE id={session_id} {radio} {payload.get('d', '')}"

    if packet_type == "end":
        result = payload.get("res", "?")
        passed = payload.get("pass", "?")
        total = payload.get("total", "?")
        return f"[{timestamp}] HART END id={session_id} result={result} {passed}/{total} {radio}"

    return f"[{timestamp}] {session_id} {packet_type} {radio}"


def build_arg_parser():
    parser = argparse.ArgumentParser(description="Receive LoRa calibration packets from ESP32 serial output")
    parser.add_argument("--port", required=True, help="Serial port used by the ESP32, for example COM5")
    parser.add_argument("--baud", type=int, default=115200, help="ESP32 USB serial baud rate")
    parser.add_argument("--out-dir", default="lora_sessions", help="Directory where received sessions are stored")
    parser.add_argument("--timeout", type=float, default=1.0, help="Serial read timeout in seconds")
    parser.add_argument("--sign-server-url", default="http://localhost:5000", help="Signing server base URL")
    parser.add_argument("--sign-username", default=os.environ.get("SIGN_SERVER_USERNAME", ""), help="Signing server username for POST /calib-data")
    parser.add_argument("--sign-password", default=os.environ.get("SIGN_SERVER_PASSWORD"), help="Signing server password for POST /calib-data")
    parser.add_argument("--profile-tag", default="YTA610", help="Certificate profile tag used to derive instrument metadata and range")
    return parser


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    try:
        import serial
    except ImportError:
        raise SystemExit("pyserial is required. Install it with: pip install pyserial")

    os.makedirs(args.out_dir, exist_ok=True)
    packet_log_path = os.path.join(args.out_dir, "packets.jsonl")
    text_log_path = os.path.join(args.out_dir, "text_packets.jsonl")
    collector = SessionCollector(args.out_dir)

    serial_port = serial.Serial(args.port, args.baud, timeout=args.timeout)
    try:
        serial_port.dtr = False
        serial_port.rts = False
    except Exception:
        pass
    time.sleep(1.5)
    try:
        serial_port.reset_input_buffer()
    except Exception:
        pass
    print(f"Listening on {args.port} at {args.baud} baud")
    print(f"Saving completed sessions to {os.path.abspath(args.out_dir)}")
    print("Press Ctrl+C to stop.\n")

    try:
        with open(packet_log_path, "a", encoding="utf-8") as packet_log, open(text_log_path, "a", encoding="utf-8") as text_log:
            while True:
                raw_line = serial_port.readline()
                if not raw_line:
                    continue

                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue

                marker = "LORA_JSON "
                marker_pos = line.find(marker)
                if marker_pos < 0:
                    print(line)
                    continue

                candidate = line[marker_pos + len(marker):].strip()
                if not candidate.startswith("{"):
                    print(line)
                    continue

                try:
                    wrapper = json.loads(candidate)
                except json.JSONDecodeError as exc:
                    print(f"Bad ESP32 JSON line: {exc}: {line}")
                    continue

                wrapper["pc_received_at"] = datetime.now().isoformat(timespec="seconds")
                packet_log.write(json.dumps(wrapper, ensure_ascii=False) + "\n")
                packet_log.flush()

                try:
                    session_id, packet_type, payload = collector.process_wrapper(wrapper)
                except ValueError as exc:
                    print(f"Skipping packet: {exc}")
                    continue

                if packet_type in ("text", "text-row", "text-end", "hex-row", "hex-end"):
                    text_entry = {
                        "pc_received_at": wrapper["pc_received_at"],
                        "rssi": wrapper.get("rssi"),
                        "snr": wrapper.get("snr"),
                        "len": wrapper.get("len"),
                        "payload": payload,
                    }
                    text_log.write(json.dumps(text_entry, ensure_ascii=False) + "\n")
                    text_log.flush()
                    if packet_type == "text":
                        print(f"[{wrapper['pc_received_at']}] TEXT RSSI={wrapper.get('rssi')}dBm SNR={wrapper.get('snr')}dB {payload}")
                    else:
                        print(format_hart_packet(wrapper, session_id, packet_type, payload))
                        if packet_type in ("text-end", "hex-end") and payload.get("complete"):
                            path = collector.save_session(session_id)
                            print(f"Session complete: COMPLETE -> {path}\n")
                            try:
                                handle_completed_session(path, args)
                            except Exception as exc:
                                print(f"Signing handoff failed: {exc}")
                    continue

                if packet_type == "json" and session_id is None:
                    print(f"[{wrapper['pc_received_at']}] JSON {json.dumps(payload, ensure_ascii=False)}")
                    continue

                if packet_type == "ack":
                    continue

                if packet_type == "duplicate":
                    continue

                if packet_type == "fragment":
                    continue

                print(format_hart_packet(wrapper, session_id, packet_type, payload))

                if packet_type in ("end", "single"):
                    path = collector.save_session(session_id)
                    result = payload.get("res", "SINGLE")
                    print(f"Session complete: {result} -> {path}\n")
                    try:
                        handle_completed_session(path, args)
                    except Exception as exc:
                        print(f"Signing handoff failed: {exc}")

    except KeyboardInterrupt:
        print("\nStopping receiver. Saving partial sessions...")
        collector.save_all()
    finally:
        serial_port.close()


if __name__ == "__main__":
    main()
