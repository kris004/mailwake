CARGO ?= cargo
INSTALL ?= install
SED ?= sed
PREFIX ?= $(HOME)/.local
BINDIR ?= $(PREFIX)/bin
SYSTEMD_USER_DIR ?= $(HOME)/.config/systemd/user
BIN := mailwake
RELEASE_BIN := target/release/$(BIN)

ifeq ($(BINDIR),$(HOME)/.local/bin)
SYSTEMD_EXEC_START ?= %h/.local/bin/$(BIN)
else
SYSTEMD_EXEC_START ?= $(BINDIR)/$(BIN)
endif

.PHONY: all build release install uninstall install-systemd install-systemd-hardened uninstall-systemd \
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
	$(SED) 's|^ExecStart=.*|ExecStart=$(SYSTEMD_EXEC_START) --config %h/.config/mailwake/config.toml|' \
		contrib/systemd/mailwake.service > "$(DESTDIR)$(SYSTEMD_USER_DIR)/mailwake.service"
	chmod 0644 "$(DESTDIR)$(SYSTEMD_USER_DIR)/mailwake.service"

install-systemd-hardened:
	$(INSTALL) -d "$(DESTDIR)$(SYSTEMD_USER_DIR)"
	$(SED) 's|^ExecStart=.*|ExecStart=$(SYSTEMD_EXEC_START) --config %h/.config/mailwake/config.toml|' \
		contrib/systemd/mailwake-hardened.service > "$(DESTDIR)$(SYSTEMD_USER_DIR)/mailwake.service"
	chmod 0644 "$(DESTDIR)$(SYSTEMD_USER_DIR)/mailwake.service"

uninstall-systemd:
	rm -f "$(DESTDIR)$(SYSTEMD_USER_DIR)/mailwake.service"

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
