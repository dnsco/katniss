#list just tasks
default:
    just -l

# install deps necessary to run these scripts
install_cli_deps:
    cargo install fd

# Clean out data dir, run all crates test suites and dump test parquests to stdout
test:
    just katniss-test/clean
    cargo test
    just katniss-test/print_test_parquets
