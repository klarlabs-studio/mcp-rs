// Derived from rmcp 3.5.1; maintained and modified by mcp-rs. See ../../../UPSTREAM.md.
/// # StdIO Transport
///
/// Create a pair of [`tokio::io::Stdin`] and [`tokio::io::Stdout`].
pub fn stdio() -> (tokio::io::Stdin, tokio::io::Stdout) {
    (tokio::io::stdin(), tokio::io::stdout())
}
