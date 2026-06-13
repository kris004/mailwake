CARGO ?= cargo
INSTALL ?= install
PREFIX ?= $(HOME)/.local
BINDIR ?= $(PREFIX)/bin
SYSTEMD_USER_DIR ?= $(HOME)/.config/systemd/user
BIN := mailwake
RELEASE_BIN := target/release/$(BIN)

.PHONY: all build release install uninstall install-systemd install-systemd-hardened \
	fmt fmt-check check test clippy clean

all: build

build:
	$(CARGO) build --locked

release:
	$(CARGO) build --release --locked

install: release
	$(INSTALL) -d "$(DESTDIR)$(BINDIR)"
	$(INSTALL) -m 0755 "$(RELEASE_BIN)" "$(DESTDIR)$(BINDIR)/$(BIN)"

uninstall:
	rm -f "$(DESTDIR)$(BINDIR)/$(BIN)"

install-systemd:
	$(INSTALL) -d "$(DESTDIR)$(SYSTEMD_USER_DIR)"
	$(INSTALL) -m 0644 contrib/systemd/mailwake.service "$(DESTDIR)$(SYSTEMD_USER_DIR)/mailwake.service"

install-systemd-hardened:
	$(INSTALL) -d "$(DESTDIR)$(SYSTEMD_USER_DIR)"
	$(INSTALL) -m 0644 contrib/systemd/mailwake-hardened.service "$(DESTDIR)$(SYSTEMD_USER_DIR)/mailwake.service"

fmt:
	$(CARGO) fmt

fmt-check:
	$(CARGO) fmt --check

check:
	$(CARGO) check --locked

test:
	$(CARGO) test --locked

clippy:
	$(CARGO) clippy --all-targets --all-features --locked -- -D warnings

clean:
	$(CARGO) clean
