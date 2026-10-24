"""Regenerate test-only TLS identities. Requires Python cryptography; no network."""
from datetime import datetime
from pathlib import Path
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

ROOT = Path(__file__).resolve().parent


def certificate(name, key, issuer=None, ca=False, usage=None):
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    builder = (x509.CertificateBuilder().subject_name(subject)
               .issuer_name(issuer[0].subject if issuer else subject)
               .public_key(key.public_key()).serial_number(x509.random_serial_number())
               .not_valid_before(datetime(2020, 1, 1)).not_valid_after(datetime(2120, 1, 1))
               .add_extension(x509.BasicConstraints(ca=ca, path_length=0 if ca else None), critical=True)
               .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False,
                              key_encipherment=False, data_encipherment=False, key_agreement=False,
                              key_cert_sign=ca, crl_sign=ca, encipher_only=False, decipher_only=False), critical=True))
    if usage:
        builder = builder.add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
    if usage == ExtendedKeyUsageOID.SERVER_AUTH:
        builder = builder.add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
    return builder.sign(issuer[1] if issuer else key, hashes.SHA256())


def save_cert(name, cert):
    (ROOT / (name + ".pem")).write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def main():
    key = ec.generate_private_key(ec.SECP256R1())
    ca = certificate("mcp-rs test CA", key, ca=True)
    other_key = ec.generate_private_key(ec.SECP256R1())
    other_ca = certificate("mcp-rs untrusted test CA", other_key, ca=True)
    save_cert("ca", ca)
    save_cert("untrusted-ca", other_ca)
    for name, issuer, usage in [
        ("server", (ca, key), ExtendedKeyUsageOID.SERVER_AUTH),
        ("client", (ca, key), ExtendedKeyUsageOID.CLIENT_AUTH),
        ("untrusted-client", (other_ca, other_key), ExtendedKeyUsageOID.CLIENT_AUTH),
    ]:
        leaf_key = ec.generate_private_key(ec.SECP256R1())
        save_cert(name, certificate(name, leaf_key, issuer, usage=usage))
        (ROOT / (name + "-key.pem")).write_bytes(leaf_key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))


if __name__ == "__main__":
    main()
