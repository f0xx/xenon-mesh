# Debian and Ubuntu

This directory is a source-package skeleton. It is not at the repository root,
so `debuild` from the root will not see it.

```sh
# From a clean checkout, with cargo and rustc installed:
ln -s packaging/debian debian
dpkg-buildpackage -us -uc -b
```

The first build downloads crates. Later builds can use the Cargo cache.
`node.id` is set in `/etc/xmeshd/local.yaml` after install, not by the package.
An OpenRC script is installed under `/usr/share/xmeshd/` for hosts that do not
use systemd. A systemd unit is not generated here.
