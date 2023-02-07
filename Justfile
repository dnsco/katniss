#list just tasks
default:
    just -l

# install deps necessary to run these scripts
install_cli_deps:
    cargo install fd

# Clean out data dir, run all crates test suites and dump test parquests to stdout
test:
    just prost-arrow-test/clean
    cargo test
    just prost-arrow-test/print_test_parquets
