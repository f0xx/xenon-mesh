//! Per-node identity.
//!
//! Each node has a key file. The hub keeps a copy per node id. A mutating
//! call carries the node id, a millisecond timestamp, and a BLAKE3 MAC over
//! a canonical string. This is not TLS. It stops a peer from casually
//! claiming another node's id once the hub has that node's key.

use std::collections::HashMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn key_from_file(path: &Path) -> Option<[u8; 32]> {
    let bytes = std::fs::read(path).ok()?;
    if bytes.is_empty() {
        return None;
    }
    Some(*blake3::hash(&bytes).as_bytes())
}

pub fn load_peers(dir: &Path) -> HashMap<String, [u8; 32]> {
    let mut out = HashMap::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return out;
    };
    for ent in rd.flatten() {
        let name = ent.file_name().to_string_lossy().to_string();
        let Some(id) = name.strip_suffix(".key") else {
            continue;
        };
        if id.is_empty() {
            continue;
        }
        if let Some(key) = key_from_file(&ent.path()) {
            out.insert(id.to_string(), key);
        }
    }
    out
}

pub fn new_token() -> (String, String) {
    let mut buf = [0u8; 16];
    let filled = std::fs::File::open("/dev/urandom")
        .and_then(|mut file| std::io::Read::read_exact(&mut file, &mut buf))
        .is_ok();
    if !filled {
        buf.copy_from_slice(&blake3::hash(b"xmesh-invite").as_bytes()[..16]);
    }
    let token = buf.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    let hash = blake3::hash(token.as_bytes()).to_hex().to_string();
    (token, hash)
}

pub fn canonical(kind: &str, parts: &[&str]) -> Vec<u8> {
    let mut text = kind.to_string();
    for part in parts {
        text.push('\n');
        text.push_str(part);
    }
    text.into_bytes()
}

pub fn sign(key: &[u8; 32], node_id: &str, ts_ms: i64, body: &[u8]) -> String {
    let material = format!("{node_id}:{ts_ms}:{}", blake3::hash(body).to_hex());
    blake3::keyed_hash(key, material.as_bytes()).to_hex().to_string()
}

pub fn verify(key: &[u8; 32], node_id: &str, ts_ms: i64, body: &[u8], sig_hex: &str) -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    if (now - ts_ms).abs() > 5 * 60 * 1000 {
        return false;
    }
    let expected = sign(key, node_id, ts_ms, body);
    subtle_eq(expected.as_bytes(), sig_hex.as_bytes())
}

fn subtle_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// When the hub has no peer keys, the claimed id is accepted.
/// When it has any, the signature has to match that node's key.
pub fn resolve_peer(
    peers: &HashMap<String, [u8; 32]>,
    node_id: &str,
    ts_ms: i64,
    sig_hex: &str,
    body: &[u8],
) -> Result<String, &'static str> {
    if peers.is_empty() {
        return Ok(if node_id.is_empty() {
            "unknown".into()
        } else {
            node_id.to_string()
        });
    }
    let key = peers.get(node_id).ok_or("unknown node")?;
    if !verify(key, node_id, ts_ms, body, sig_hex) {
        return Err("node signature rejected");
    }
    Ok(node_id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_rejects_a_different_node_and_accepts_the_owner() {
        let key = [7u8; 32];
        let body = canonical("append", &["general", "main", "user", "hello"]);
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let sig = sign(&key, "node-a", ts, &body);
        assert!(verify(&key, "node-a", ts, &body, &sig));
        assert!(!verify(&key, "node-b", ts, &body, &sig));
        let mut peers = HashMap::new();
        peers.insert("node-a".into(), key);
        assert!(resolve_peer(&peers, "node-b", ts, &sig, &body).is_err());
        assert_eq!(
            resolve_peer(&peers, "node-a", ts, &sig, &body).unwrap(),
            "node-a"
        );
    }
}
