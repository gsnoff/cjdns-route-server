# Open source high performance cjdns Route Server

[![Build Status](https://travis-ci.org/CJDNS-Development-Team/CJDNS.svg?branch=master)](https://travis-ci.org/CJDNS-Development-Team/CJDNS)

## Build & test

Debug build:

```bash
cargo test
```

Release build:

```bash
cargo build --release
```

## Docs

```bash
cargo doc --no-deps
```

### Individual crates

[**cjdns-core**](cjdns-core/) - core CJDNS types and algorithms:
- tools for manipulating and splicing cjdns routing labels;
- serializing/deserializing of encoding schemes;
- public & private keys.

## Development

Formatting code:

```bash
cargo fmt
```
