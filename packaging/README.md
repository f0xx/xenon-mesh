# Packaging

Skeletons for installing `xmeshd`. They build the release binary from this
repository. They do not vendor the Cargo crate index; the build machine needs
network access the first time, or a populated Cargo cache.

The binary is statically linked apart from the system dynamic loader. Git is
the runtime dependency the scripts use. A model server is separate and is not
part of the package.

| Directory | System |
| --- | --- |
| `gentoo/` | Live ebuild (`xmeshd-9999`) and an OpenRC script from `openrc/` |
| `debian/` | Source package layout for Debian and Ubuntu |
| `alpine/` | `APKBUILD` |
| `openrc/` | Service script used by the Gentoo ebuild and usable on its own |

Copy `configs/local.master.example.yaml` to `/etc/xmeshd/local.yaml` after
install and set `node.id`. The daemon refuses a blank id.
