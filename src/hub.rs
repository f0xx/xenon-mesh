use crate::storage::pb::cluster_service_client::ClusterServiceClient;
use crate::storage::pb::{
    ChannelRecord, ContextQuery, ListChannelsRequest, SessionMessage, SyncEnvelope,
};
use anyhow::Result;

pub async fn list_public(master_url: &str) -> Result<Vec<ChannelRecord>> {
    let mut client = ClusterServiceClient::connect(master_url.to_string()).await?;
    Ok(client
        .list_channels(ListChannelsRequest {})
        .await?
        .into_inner()
        .channels)
}

pub async fn append_public(master_url: &str, msg: SessionMessage) -> Result<SessionMessage> {
    let mut client = ClusterServiceClient::connect(master_url.to_string()).await?;
    Ok(client.append_message(msg).await?.into_inner())
}

pub async fn query_namespace(master_url: &str, namespace: &str) -> Result<SyncEnvelope> {
    let mut client = ClusterServiceClient::connect(master_url.to_string()).await?;
    let req = ContextQuery {
        namespace: namespace.to_string(),
        ..Default::default()
    };
    Ok(client.query_node_context(req).await?.into_inner())
}

pub async fn push_envelope(master_url: &str, env: SyncEnvelope) -> Result<()> {
    let mut client = ClusterServiceClient::connect(master_url.to_string()).await?;
    client.sync_contexts(env).await?;
    Ok(())
}
