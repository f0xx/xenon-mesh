//! Mutual TLS material for gRPC and the mesh HTTP listener.
//!
//! The files are PEM. The CA signs every node certificate. A connection
//! without a certificate from that CA is refused. This is separate from the
//! application MAC in `identity`.

use crate::config::NodeSection;
use anyhow::{Context, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::RootCertStore;
use std::io::Cursor;
use std::sync::Arc;

#[derive(Clone)]
pub struct Material {
    pub ca_pem: String,
    pub cert_pem: String,
    pub key_pem: String,
}

impl Material {
    pub fn identity_pem(&self) -> String {
        format!("{}\n{}", self.cert_pem.trim(), self.key_pem.trim())
    }
}

pub fn load(node: &NodeSection) -> Option<Material> {
    let ca = node.mtls_ca_path.as_deref()?.trim();
    let cert = node.mtls_cert_path.as_deref()?.trim();
    let key = node.mtls_key_path.as_deref()?.trim();
    if ca.is_empty() || cert.is_empty() || key.is_empty() {
        return None;
    }
    let ca_pem = std::fs::read_to_string(ca).ok()?;
    let cert_pem = std::fs::read_to_string(cert).ok()?;
    let key_pem = std::fs::read_to_string(key).ok()?;
    if ca_pem.trim().is_empty() || cert_pem.trim().is_empty() || key_pem.trim().is_empty() {
        return None;
    }
    Some(Material {
        ca_pem,
        cert_pem,
        key_pem,
    })
}

pub fn server_config(mat: &Material) -> Result<Arc<rustls::ServerConfig>> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let mut roots = RootCertStore::empty();
    for ca in rustls_pemfile::certs(&mut Cursor::new(mat.ca_pem.as_bytes())) {
        roots.add(ca.context("mesh CA")?)?;
    }
    if roots.is_empty() {
        anyhow::bail!("mesh CA file has no certificates");
    }
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut Cursor::new(mat.cert_pem.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
            .context("mesh certificate")?;
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut Cursor::new(mat.key_pem.as_bytes()))?
        .context("mesh private key")?;
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .context("client certificate verifier")?;
    let config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .context("mesh server certificate")?;
    Ok(Arc::new(config))
}

pub fn tonic_server(mat: &Material) -> Result<tonic::transport::ServerTlsConfig> {
    Ok(tonic::transport::ServerTlsConfig::new()
        .identity(tonic::transport::Identity::from_pem(
            &mat.cert_pem,
            &mat.key_pem,
        ))
        .client_ca_root(tonic::transport::Certificate::from_pem(&mat.ca_pem)))
}

pub fn tonic_client(mat: &Material) -> Result<tonic::transport::ClientTlsConfig> {
    Ok(tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(&mat.ca_pem))
        .identity(tonic::transport::Identity::from_pem(
            &mat.cert_pem,
            &mat.key_pem,
        )))
}
