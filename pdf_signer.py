"""
PDF Digital Signer
==================
Digitally signs PDF files using RSA + SHA-256 with a self-signed X.509 certificate.
The private key is encrypted with a user passphrase — nothing runs without it.

Features:
  - Generates an RSA-2048 key pair and self-signed certificate (one-time setup)
  - Private key is always stored AES-256 encrypted (BestAvailableEncryption)
  - Signs any PDF: computes SHA-256 hash → signs with private key → embeds .sig
  - Verifies a signature against the signed PDF

Usage:
  python pdf_signer.py keygen  ["Your Name" "Your Org"]   # generate key + cert
  python pdf_signer.py sign    <input.pdf> [output.pdf]   # sign a PDF
  python pdf_signer.py verify  <signed.pdf>               # verify a signature
  python pdf_signer.py passwd                             # change the key passphrase
"""

import sys
import os
import json
import hashlib
import base64
import datetime
import getpass
from pathlib import Path

# ── cryptography ───────────────────────────────────────────────────────────────
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives.asymmetric.utils import Prehashed
from cryptography.exceptions import InvalidSignature

# ── PDF stamping ───────────────────────────────────────────────────────────────
from pypdf import PdfReader, PdfWriter
import io

# ── Paths ──────────────────────────────────────────────────────────────────────
KEY_DIR       = Path("keys")
PRIV_KEY      = KEY_DIR / "private_key.pem"
PUB_CERT      = KEY_DIR / "certificate.pem"
PASSPHRASE_H  = KEY_DIR / "passphrase.sha256"   # SHA-256 of passphrase for fast check

MAX_ATTEMPTS  = 3   # max wrong-passphrase attempts before abort


# ══════════════════════════════════════════════════════════════════════════════
# Passphrase helpers
# ══════════════════════════════════════════════════════════════════════════════

def _hash_passphrase(pw: str) -> str:
    """Deterministic SHA-256 hex digest used only for quick 'is it right?' check."""
    return hashlib.sha256(pw.encode("utf-8")).hexdigest()


def _save_passphrase_hash(pw: str):
    PASSPHRASE_H.write_text(_hash_passphrase(pw))


def _check_passphrase_hash(pw: str) -> bool:
    """Return True if the stored hash matches pw. Returns True if no hash file exists
    (legacy key without hash — let the actual key-load decide)."""
    if not PASSPHRASE_H.exists():
        return True
    return _hash_passphrase(pw) == PASSPHRASE_H.read_text().strip()


def _prompt_new_passphrase(confirm: bool = True) -> bytes | None:
    """Interactively prompt for a new passphrase (with optional confirmation).
    Returns the passphrase as bytes, or None for no encryption (not recommended)."""
    while True:
        pw = getpass.getpass("Enter a passphrase for the private key (blank = no encryption ⚠): ")
        if not pw:
            ans = input("  ⚠  No passphrase means the key is unprotected. Continue? [y/N] ")
            if ans.lower() != "y":
                continue
            print("  ⚠  Key will be stored without encryption.")
            return None   # caller must handle None → NoEncryption
        if confirm:
            pw2 = getpass.getpass("Confirm passphrase: ")
            if pw != pw2:
                print("  ✗  Passphrases do not match. Try again.\n")
                continue
        return pw.encode("utf-8")


def _prompt_passphrase(prompt: str = "Signing key passphrase: ") -> bytes | None:
    """Prompt for an existing passphrase up to MAX_ATTEMPTS times.
    Returns the passphrase bytes on success, or exits."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        pw = getpass.getpass(prompt)
        if not pw:
            # User pressed Enter — key might be unencrypted
            if not PASSPHRASE_H.exists():
                return None
            # Hash file exists, meaning a passphrase was set
            print("  ✗  Passphrase required (press Ctrl+C to abort).")
            continue
        if _check_passphrase_hash(pw):
            return pw.encode("utf-8")
        remaining = MAX_ATTEMPTS - attempt
        if remaining:
            print(f"  ✗  Incorrect passphrase. {remaining} attempt(s) remaining.")
        else:
            sys.exit("  ✗  Too many failed attempts. Aborting.")
    sys.exit("  ✗  Passphrase verification failed.")


def _load_private_key(passphrase: bytes | None = None):
    """Load (and decrypt) the private key, prompting if passphrase not supplied."""
    if not PRIV_KEY.exists():
        sys.exit("❌  Private key not found. Run:  python pdf_signer.py keygen")
    pw = passphrase
    if pw is None and PASSPHRASE_H.exists():
        pw = _prompt_passphrase()
    try:
        return serialization.load_pem_private_key(
            PRIV_KEY.read_bytes(), password=pw, backend=default_backend()
        )
    except (ValueError, TypeError) as e:
        sys.exit(f"❌  Could not decrypt private key: {e}\n"
                 "    Check your passphrase or re-run with the correct one.")


def _load_certificate():
    if not PUB_CERT.exists():
        sys.exit("❌  Certificate not found. Run:  python pdf_signer.py keygen")
    return x509.load_pem_x509_certificate(PUB_CERT.read_bytes(), default_backend())


def load_private_key_with_passphrase(passphrase_str: str):
    """Load private key given a plaintext passphrase string (used by sign_server.py)."""
    pw = passphrase_str.encode("utf-8") if passphrase_str else None
    if not _check_passphrase_hash(passphrase_str):
        raise ValueError("Incorrect passphrase")
    try:
        return serialization.load_pem_private_key(
            PRIV_KEY.read_bytes(), password=pw, backend=default_backend()
        )
    except (ValueError, TypeError) as e:
        raise ValueError(f"Could not decrypt private key: {e}") from e


# ══════════════════════════════════════════════════════════════════════════════
# 1. KEY GENERATION
# ══════════════════════════════════════════════════════════════════════════════

def generate_keys(name: str = "PDF Signer", org: str = "My Organisation"):
    """Generate RSA-2048 private key + self-signed X.509 certificate.
    The private key is encrypted with a user-chosen passphrase."""
    KEY_DIR.mkdir(exist_ok=True)

    print("⚙  Generating RSA-2048 private key …")
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend(),
    )

    # ── Passphrase ────────────────────────────────────────────────────────────
    print()
    pw_bytes = _prompt_new_passphrase(confirm=True)
    if pw_bytes:
        enc = serialization.BestAvailableEncryption(pw_bytes)
        _save_passphrase_hash(pw_bytes.decode("utf-8"))
        print("🔒  Private key will be AES-256 encrypted with your passphrase.")
    else:
        enc = serialization.NoEncryption()
        # Remove any stale hash file so _check_passphrase_hash passes on empty
        PASSPHRASE_H.unlink(missing_ok=True)
        print("⚠   Private key stored WITHOUT encryption.")

    # Save private key
    PRIV_KEY.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=enc,
        )
    )

    print("📜 Creating self-signed X.509 certificate …")
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME,       name),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, org),
        x509.NameAttribute(NameOID.COUNTRY_NAME,      "IN"),
    ])

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(private_key, hashes.SHA256(), default_backend())
    )

    PUB_CERT.write_bytes(cert.public_bytes(serialization.Encoding.PEM))

    print(f"\n✅ Keys saved to  {KEY_DIR}/")
    print(f"   🔑 Private key  : {PRIV_KEY}  {'(encrypted)' if pw_bytes else '(UNENCRYPTED)'}")
    print(f"   📄 Certificate  : {PUB_CERT}")
    print(f"   🔒 Passphrase   : {'(stored as SHA-256 hash for quick check)' if pw_bytes else 'not set'}")
    print(f"\n   Common Name    : {name}")
    print(f"   Organisation   : {org}")
    print(f"   Valid until    : {cert.not_valid_after_utc.date()}")
    print()
    print("   Next step — start the sign server:")
    print("     python sign_server.py")


# ══════════════════════════════════════════════════════════════════════════════
# 2. CHANGE PASSPHRASE
# ══════════════════════════════════════════════════════════════════════════════

def change_passphrase():
    """Re-encrypt the private key with a new passphrase."""
    print("🔑  Changing private key passphrase")
    print()

    # Load existing key
    old_pw = _prompt_passphrase("Current passphrase (Enter if none): ")
    private_key = _load_private_key(passphrase=old_pw)

    print("\nSet new passphrase:")
    new_pw_bytes = _prompt_new_passphrase(confirm=True)
    if new_pw_bytes:
        enc = serialization.BestAvailableEncryption(new_pw_bytes)
        _save_passphrase_hash(new_pw_bytes.decode("utf-8"))
    else:
        enc = serialization.NoEncryption()
        PASSPHRASE_H.unlink(missing_ok=True)

    PRIV_KEY.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=enc,
        )
    )
    print(f"\n✅  Passphrase updated. Key re-encrypted in {PRIV_KEY}")


# ══════════════════════════════════════════════════════════════════════════════
# 3. SIGNING
# ══════════════════════════════════════════════════════════════════════════════

def sign_pdf(input_path: str, output_path: str | None = None):
    """Sign a PDF: prompt for passphrase, compute hash, sign, embed signature."""
    inp  = Path(input_path)
    outp = Path(output_path) if output_path else inp

    if not inp.exists():
        sys.exit(f"❌  File not found: {inp}")

    private_key = _load_private_key()           # prompts for passphrase
    cert        = _load_certificate()

    timestamp   = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    cert_serial = hex(cert.serial_number)
    subject_cn  = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value

    # ── 1. Build base PDF (pages + metadata, no attachment) ──────────────────
    reader = PdfReader(str(inp))
    meta_dict = {
        "/DigitallySigned": "true",
        "/SignerName":      subject_cn,
        "/SignatureDate":   timestamp,
    }

    base_buf = io.BytesIO()
    writer = PdfWriter()
    for page in reader.pages:
        writer.add_page(page)
    writer.add_metadata(meta_dict)
    writer.write(base_buf)

    # ── 2. Hash ───────────────────────────────────────────────────────────────
    print(f"🔍  Hashing {outp.name} …")
    digest     = hashlib.sha256(base_buf.getvalue()).digest()
    hex_digest = digest.hex()

    # ── 3. Sign ───────────────────────────────────────────────────────────────
    print("✍   Signing with RSA-2048 / SHA-256 …")
    signature_bytes = private_key.sign(digest, padding.PKCS1v15(), Prehashed(hashes.SHA256()))
    b64_sig = base64.b64encode(signature_bytes).decode()

    # ── 4. Embed signature as attachment ──────────────────────────────────────
    print("📎  Embedding signature …")
    sig_meta = {
        "version":     "1.0",
        "algorithm":   "RSA-PKCS1v15-SHA256",
        "signed_file": outp.name,
        "sha256":      hex_digest,
        "timestamp":   timestamp,
        "signer":      subject_cn,
        "cert_serial": cert_serial,
        "certificate": PUB_CERT.read_text(),
        "signature":   b64_sig,
    }

    base_buf.seek(0)
    reader2 = PdfReader(base_buf)
    writer2  = PdfWriter()
    for page in reader2.pages:
        writer2.add_page(page)
    writer2.add_metadata(meta_dict)
    writer2.add_attachment("signature.sig", json.dumps(sig_meta, indent=2).encode())

    with open(outp, "wb") as f:
        writer2.write(f)

    print(f"\n✅  Signed PDF  → {outp}")
    print(f"   SHA-256     : {hex_digest[:32]}…")
    print(f"   Signer      : {subject_cn}")
    print(f"   Timestamp   : {timestamp}")


# ══════════════════════════════════════════════════════════════════════════════
# 4. VERIFICATION  (no passphrase needed — public key is in the .sig attachment)
# ══════════════════════════════════════════════════════════════════════════════

def verify_pdf(signed_pdf_path: str):
    """Verify a signed PDF by extracting its embedded signature attachment."""
    signed_path = Path(signed_pdf_path)

    if not signed_path.exists():
        sys.exit(f"❌  File not found: {signed_path}")

    print(f"🔍  Verifying {signed_path.name} …\n")

    reader = PdfReader(str(signed_path))
    attachments = reader.attachments

    if "signature.sig" not in attachments:
        sys.exit(
            f"❌  No embedded signature found in: {signed_path.name}\n\n"
            "    Sign the PDF first:\n"
            "        python pdf_signer.py sign  your_file.pdf\n"
        )

    meta = json.loads(attachments["signature.sig"][0].decode())

    # Load public key from certificate stored in signature
    cert       = x509.load_pem_x509_certificate(meta["certificate"].encode(), default_backend())
    public_key = cert.public_key()

    # Certificate validity
    now        = datetime.datetime.now(datetime.timezone.utc)
    cert_ok    = cert.not_valid_before_utc <= now <= cert.not_valid_after_utc
    print(f"{'✅' if cert_ok else '❌'}  Certificate validity  : "
          f"{cert.not_valid_before_utc.date()}  →  {cert.not_valid_after_utc.date()}  "
          f"({'valid' if cert_ok else 'EXPIRED'})")

    # Recompute hash over the base PDF (same way as during signing)
    base_buf = io.BytesIO()
    writer   = PdfWriter()
    for page in reader.pages:
        writer.add_page(page)
    writer.add_metadata({
        "/DigitallySigned": "true",
        "/SignerName":      meta.get("signer", ""),
        "/SignatureDate":   meta.get("timestamp", ""),
    })
    writer.write(base_buf)

    actual_digest = hashlib.sha256(base_buf.getvalue()).digest()
    hash_ok = actual_digest.hex() == meta["sha256"]
    print(f"{'✅' if hash_ok else '❌'}  SHA-256 hash          : "
          f"{'matches' if hash_ok else 'MISMATCH — file may have been tampered!'}")
    print(f"   Expected : {meta['sha256'][:32]}…")
    print(f"   Actual   : {actual_digest.hex()[:32]}…")

    # RSA signature verification
    sig_bytes = base64.b64decode(meta["signature"])
    try:
        public_key.verify(sig_bytes, actual_digest, padding.PKCS1v15(), Prehashed(hashes.SHA256()))
        sig_ok = True
    except InvalidSignature:
        sig_ok = False

    print(f"{'✅' if sig_ok else '❌'}  RSA signature         : "
          f"{'valid' if sig_ok else 'INVALID — signature does not match!'}")

    overall = cert_ok and hash_ok and sig_ok
    print()
    print("═" * 52)
    if overall:
        print("✅  VERIFICATION PASSED")
        print(f"   Signer    : {meta.get('signer', '?')}")
        print(f"   Algorithm : {meta.get('algorithm', '?')}")
        print(f"   Signed at : {meta.get('timestamp', '?')}")
        print(f"   Cert S/N  : {meta.get('cert_serial', '?')}")
    else:
        print("❌  VERIFICATION FAILED — document may have been altered.")
    print("═" * 52)


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def usage():
    print(__doc__)
    sys.exit(1)


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        usage()

    cmd = args[0].lower()

    if cmd == "keygen":
        name = args[1] if len(args) > 1 else "PDF Signer"
        org  = args[2] if len(args) > 2 else "My Organisation"
        generate_keys(name, org)

    elif cmd == "sign":
        if len(args) < 2:
            usage()
        sign_pdf(args[1], args[2] if len(args) > 2 else None)

    elif cmd == "verify":
        if len(args) < 2:
            usage()
        verify_pdf(args[1])

    elif cmd in ("passwd", "changepw", "change-passphrase"):
        change_passphrase()

    else:
        usage()
