#![recursion_limit = "256"]

#[path = "../macro_executor.rs"]
mod macro_executor;

fn main() {
    if let Err(error) = macro_executor::run_cli(std::env::args().skip(1)) {
        eprintln!("codex-native-macro: {error:#}");
        std::process::exit(1);
    }
}
