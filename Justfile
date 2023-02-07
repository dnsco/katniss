default:
    just -l

install_cli_deps:
    cargo install fd

test:
    just prost-arrow-test/clean
    cargo test
    just prost-arrow-test/print_test_parquets
