# Copyright 2026 xmeshd contributors
# Distributed under the terms of the MIT License

EAPI=8

inherit git-r3

DESCRIPTION="Local-first chat daemon for project context and model backends"
HOMEPAGE="https://github.com/f0xx/xenon-mesh"
EGIT_REPO_URI="https://github.com/f0xx/xenon-mesh.git"

# MIT covers this repository. The binary also bundles libgit2 (GPL-2.0 with
# the linking exception) and aws-lc (Apache-2.0).
LICENSE="MIT Apache-2.0 GPL-2"
SLOT="0"
KEYWORDS=""

DEPEND="virtual/rust"
RDEPEND="dev-vcs/git"
BDEPEND="virtual/rust"

src_compile() {
	export CARGO_HOME="${T}/cargo"
	cargo build --release --locked
}

src_install() {
	dobin target/release/xmeshd
	exeinto /usr/libexec/xmeshd
	doexe scripts/google-ask.sh scripts/harvest-context.sh \
		scripts/xmesh-tls.sh scripts/xmesh-tunnel.sh
	insinto /etc/xmeshd
	newins configs/local.master.example.yaml local.yaml.example
	newinitd packaging/openrc/xmeshd xmeshd
	dodoc README.md LICENSE NOTICE docs/*.md
}

pkg_postinst() {
	elog "Copy /etc/xmeshd/local.yaml.example to /etc/xmeshd/local.yaml"
	elog "and set node.id before starting the service."
	elog "rc-service xmeshd start"
}
