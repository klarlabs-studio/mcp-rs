//! Checked-in test identities for the interoperability example; never production credentials.
use mcp_rs::transport::websocket::tokio_rustls::rustls::{
    self,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use std::sync::Arc;

type Error = Box<dyn std::error::Error + Send + Sync>;
pub const CA: &[u8] = include_bytes!("../../tests/fixtures/tls/ca.pem");
pub const OTHER_CA: &[u8] = include_bytes!("../../tests/fixtures/tls/untrusted-ca.pem");
pub const SERVER: &[u8] = include_bytes!("../../tests/fixtures/tls/server.pem");
pub const SERVER_KEY: &[u8] = include_bytes!("../../tests/fixtures/tls/server-key.pem");
pub const CLIENT: &[u8] = include_bytes!("../../tests/fixtures/tls/client.pem");
pub const CLIENT_KEY: &[u8] = include_bytes!("../../tests/fixtures/tls/client-key.pem");
pub const OTHER_CLIENT: &[u8] = include_bytes!("../../tests/fixtures/tls/untrusted-client.pem");
pub const OTHER_CLIENT_KEY: &[u8] =
    include_bytes!("../../tests/fixtures/tls/untrusted-client-key.pem");
fn certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, Error> {
    Ok(CertificateDer::pem_slice_iter(pem).collect::<Result<_, _>>()?)
}
fn roots(pem: &[u8]) -> Result<rustls::RootCertStore, Error> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in certs(pem)? {
        roots.add(cert)?;
    }
    Ok(roots)
}
pub fn server(mtls: bool) -> Result<Arc<rustls::ServerConfig>, Error> {
    let builder = rustls::ServerConfig::builder();
    let builder = if mtls {
        builder.with_client_cert_verifier(
            rustls::server::WebPkiClientVerifier::builder(Arc::new(roots(CA)?)).build()?,
        )
    } else {
        builder.with_no_client_auth()
    };
    Ok(Arc::new(builder.with_single_cert(
        certs(SERVER)?,
        PrivateKeyDer::from_pem_slice(SERVER_KEY)?,
    )?))
}
pub fn client(identity: &str) -> Result<Arc<rustls::ClientConfig>, Error> {
    let builder = rustls::ClientConfig::builder()
        .with_root_certificates(roots(if identity == "wrong-ca" { OTHER_CA } else { CA })?);
    Ok(Arc::new(match identity {
        "trusted" => builder
            .with_client_auth_cert(certs(CLIENT)?, PrivateKeyDer::from_pem_slice(CLIENT_KEY)?)?,
        "untrusted" => builder.with_client_auth_cert(
            certs(OTHER_CLIENT)?,
            PrivateKeyDer::from_pem_slice(OTHER_CLIENT_KEY)?,
        )?,
        "missing" | "wrong-ca" | "wrong-host" => builder.with_no_client_auth(),
        _ => return Err("unknown test identity".into()),
    }))
}
