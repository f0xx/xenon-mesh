//! Neighbour discovery on the local network.
//!
//! The daemon advertises `_xmeshd._tcp` and browses the same type. A neighbour
//! is another daemon that can open a peer chat. Discovery is best-effort: a
//! network that blocks multicast simply shows an empty list.

use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::warn;

#[derive(Clone, Debug, Serialize)]
pub struct Neighbor {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub base: String,
}

pub fn spawn(node_id: String, port: u16, slot: Arc<Mutex<Vec<Neighbor>>>) {
    std::thread::spawn(move || {
        if let Err(err) = run(&node_id, port, slot) {
            warn!("neighbour discovery stopped: {err}");
        }
    });
}

fn run(node_id: &str, port: u16, slot: Arc<Mutex<Vec<Neighbor>>>) -> anyhow::Result<()> {
    let daemon = ServiceDaemon::new()?;
    let service_type = "_xmeshd._tcp.local.";
    let instance = sanitize(node_id);
    let host = format!("{instance}.local.");
    if let Some(ip) = local_ip() {
        let props = [("node", node_id)];
        match ServiceInfo::new(service_type, &instance, &host, &ip, port, &props[..]) {
            Ok(info) => {
                if let Err(err) = daemon.register(info) {
                    warn!("could not advertise this node: {err}");
                }
            }
            Err(err) => warn!("could not build the discovery record: {err}"),
        }
    }
    let receiver = daemon.browse(service_type)?;
    let mut found: HashMap<String, Neighbor> = HashMap::new();
    while let Ok(event) = receiver.recv() {
        match event {
            ServiceEvent::ServiceResolved(info) => {
                let name = info
                    .get_property_val_str("node")
                    .unwrap_or_else(|| info.get_fullname())
                    .to_string();
                if name == node_id {
                    continue;
                }
                let host = info.get_addresses().iter().next().map(|ip| ip.to_string());
                let Some(host) = host else {
                    continue;
                };
                let port = info.get_port();
                let base = format!("http://{host}:{port}");
                found.insert(
                    name.clone(),
                    Neighbor {
                        name,
                        host,
                        port,
                        base,
                    },
                );
                publish(&slot, &found);
            }
            ServiceEvent::ServiceRemoved(_, fullname) => {
                found.retain(|_, n| !fullname.contains(&n.name));
                publish(&slot, &found);
            }
            _ => {}
        }
    }
    Ok(())
}

fn publish(slot: &Mutex<Vec<Neighbor>>, found: &HashMap<String, Neighbor>) {
    let mut rows: Vec<_> = found.values().cloned().collect();
    rows.sort_by(|a, b| a.name.cmp(&b.name));
    if let Ok(mut guard) = slot.lock() {
        *guard = rows;
    }
}

fn sanitize(name: &str) -> String {
    let clean: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let clean = clean.trim_matches('-').to_string();
    if clean.is_empty() {
        "xmeshd".into()
    } else {
        clean.chars().take(40).collect()
    }
}

fn local_ip() -> Option<String> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    match socket.local_addr().ok()?.ip() {
        std::net::IpAddr::V4(ip) if !ip.is_loopback() => Some(ip.to_string()),
        _ => None,
    }
}
