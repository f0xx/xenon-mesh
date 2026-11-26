# Alpine

`APKBUILD` builds `xmeshd` with the system Cargo. The checksum is `SKIP` until
a release tarball is pinned.

```sh
git archive --format=tar.gz --prefix=xmeshd-0.1.0/ -o packaging/alpine/xmeshd-0.1.0.tar.gz HEAD
cd packaging/alpine
abuild -r
```

`abuild` needs a signing key (`abuild-keygen -a -i`) and the `cargo` and `rust`
packages. The init script is OpenRC, which Alpine uses. Set `node.id` in
`/etc/xmeshd/local.yaml` before `rc-service xmeshd start`.
