use crate::storage::pb::cluster_service_client::ClusterServiceClient;
use crate::storage::pb::{
    ChannelRecord, ContextQuery, ListChannelsRequest, SessionMessage, SyncEnvelope,
};
use anyhow::Result;
use std::time::Duration;

fn endpoint(master_url: &str) -> Result<tonic::transport::Endpoint> {
    Ok(
        tonic::transport::Endpoint::from_shared(master_url.to_string())?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30)),
    )
}

pub async fn list_public(master_url: &str) -> Result<Vec<ChannelRecord>> {
    let mut client = ClusterServiceClient::connect(endpoint(master_url)?).await?;
    Ok(client
        .list_channels(ListChannelsRequest {})
        .await?
        .into_inner()
        .channels)
}

pub async fn append_public(
    master_url: &str,
    msg: SessionMessage,
    node_id: &str,
    node_key: Option<&[u8; 32]>,
) -> Result<SessionMessage> {
    let mut client = ClusterServiceClient::connect(endpoint(master_url)?).await?;
    let body = crate::identity::canonical(
        "append",
        &[&msg.channel_id, &msg.thread_id, &msg.author, &msg.content],
    );
    let ts = crate::storage::now_ms();
    let mut req = tonic::Request::new(msg);
    if let Ok(value) = node_id.parse() {
        req.metadata_mut().insert("x-xmesh-node", value);
    }
    if let Some(key) = node_key {
        let sig = crate::identity::sign(key, node_id, ts, &body);
        if let Ok(value) = ts.to_string().parse() {
            req.metadata_mut().insert("x-xmesh-ts", value);
        }
        if let Ok(value) = sig.parse() {
            req.metadata_mut().insert("x-xmesh-sig", value);
        }
    }
    Ok(client.append_message(req).await?.into_inner())
}

pub async fn query_namespace(master_url: &str, namespace: &str) -> Result<SyncEnvelope> {
    let mut client = ClusterServiceClient::connect(endpoint(master_url)?).await?;
    let req = ContextQuery {
        namespace: namespace.to_string(),
        ..Default::default()
    };
    Ok(client.query_node_context(req).await?.into_inner())
}

/// MAC body for `SyncContexts`. Counts alone would let a replay swap the payload.
pub fn sync_body(env: &SyncEnvelope) -> Vec<u8> {
    let mut raw = Vec::new();
    raw.extend(env.source_node.as_bytes());
    raw.push(0);
    for e in &env.entries {
        raw.extend(e.id.as_bytes());
        raw.push(0);
        raw.extend(e.namespace.as_bytes());
        raw.push(0);
        raw.extend(e.content_hash.as_bytes());
        raw.push(0);
        raw.extend(e.content.as_bytes());
        raw.push(0);
    }
    for m in &env.messages {
        raw.extend(m.channel_id.as_bytes());
        raw.push(0);
        raw.extend(m.thread_id.as_bytes());
        raw.push(0);
        raw.extend(m.author.as_bytes());
        raw.push(0);
        raw.extend(m.content.as_bytes());
        raw.push(0);
    }
    for c in &env.channels {
        raw.extend(c.name.as_bytes());
        raw.push(0);
        raw.extend(c.visibility.as_bytes());
        raw.push(0);
        raw.extend(c.namespace.as_bytes());
        raw.push(0);
    }
    let digest = blake3::hash(&raw).to_hex().to_string();
    crate::identity::canonical("sync", &[&env.source_node, &digest])
}

pub async fn push_envelope(
    master_url: &str,
    env: SyncEnvelope,
    node_id: &str,
    node_key: Option<&[u8; 32]>,
) -> Result<()> {
    let mut client = ClusterServiceClient::connect(endpoint(master_url)?).await?;
    let body = sync_body(&env);
    let ts = crate::storage::now_ms();
    let mut req = tonic::Request::new(env);
    if let Ok(value) = node_id.parse() {
        req.metadata_mut().insert("x-xmesh-node", value);
    }
    if let Some(key) = node_key {
        let sig = crate::identity::sign(key, node_id, ts, &body);
        if let Ok(value) = ts.to_string().parse() {
            req.metadata_mut().insert("x-xmesh-ts", value);
        }
        if let Ok(value) = sig.parse() {
            req.metadata_mut().insert("x-xmesh-sig", value);
        }
    }
    client.sync_contexts(req).await?;
    Ok(())
}
