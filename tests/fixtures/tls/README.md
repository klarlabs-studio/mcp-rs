# TLS test identities

These public test fixtures exercise native and bidirectional Go/Rust gRPC/WebSocket TLS and mutual TLS on loopback.
The localhost server and trusted client use `ca.pem`; the rejected client uses
`untrusted-ca.pem`. Server SAN is `localhost`, and leaves have separate server/client
extended key usages. All certificates are valid from 2020-01-01 to 2120-01-01.

Private leaf keys here are test data. CA signing keys are generated in memory and
are not retained. Regenerate the entire set with Python's `cryptography` package:

```sh
python3 tests/fixtures/tls/generate.py
```

Normal builds and tests read the checked-in PEM files and require neither Python,
OpenSSL, external services nor a network connection beyond loopback listeners.
