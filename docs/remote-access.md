# Reaching the hub from outside its LAN

A VPN that routes the hub's addresses is enough, and then the node config does not change. When the VPN is not there, an SSH local forward is the flexible option: the outside machine opens SSH to the hub, and the hub's gRPC and HTTP ports appear on localhost. `xmeshd` is then pointed at those local ports. SSH is assumed to be reachable from the outside network. The daemon does not need a public listener.

## DR-42. Forwards

From the outside machine, local forwards (`ssh -L`) are the ones that matter. The node dials the hub. The hub does not dial the node.

| Local port | Hub port | Used for |
| --- | --- | --- |
| `127.0.0.1:8443` | hub gRPC | `node.master_url` |
| `127.0.0.1:8080` | hub HTTP | `node.master_http` and a browser |
| `127.0.0.1:8448` | hub HTTPS | the browser, when TLS is enabled |

A reverse forward (`ssh -R`) is only useful when the hub must open a connection back to the outside machine, for example a model server that is not on the LAN. Leave it off unless that callback exists. Model calls in the current lab are from the hub to hosts that are already beside it.

## DR-43. Helper

`scripts/xmesh-tunnel.sh` opens the forwards in the background and prints the YAML overlay. It does not edit `/etc/xmeshd/local.yaml`.

```sh
XMESH_HUB_SSH=hub.example \
XMESH_SSH_OPTS="-p 222" \
scripts/xmesh-tunnel.sh up
```

Then set the printed `master_url` and `master_http` in the local overlay and restart the node. `scripts/xmesh-tunnel.sh down` closes the control socket. `status` reports whether the socket is still alive.

The browser can use `http://127.0.0.1:8080/` through the forward. Notifications on that URL work because `127.0.0.1` is a secure context even without TLS. The HTTPS forward is there when the operator wants the same UI with the hub's certificate.

The SSH session is the trust boundary. Anyone who can authenticate to that SSH user can speak to the hub as a lab peer. That is the same rule as [security.md](security.md).
