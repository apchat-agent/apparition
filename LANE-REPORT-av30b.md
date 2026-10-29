# av30b report

The private redirect now targets `private.test` on the test server's listening port. With the real resolver guard, the fetch errors and `/private` receives zero requests.

## Red evidence

Temporarily bypassed `resolve_safe` after the first redirect hop in `src/net.rs` (the mutant was reverted and is not committed). Ran:

```text
cargo test link_card::tests::fetches_relative_og_image_and_refuses_bad_types_oversize_and_private_redirects -- --exact --test-threads=1
```

The strengthened test failed at the zero-request assertion because the redirect reached the local handler:

```text
assertion `left == right` failed: a redirect to a private address must be rejected before connecting
  left: 1
 right: 0
test result: FAILED. 0 passed; 1 failed
```

## Green evidence

With the real per-hop guard restored:

- `cargo test -- --test-threads=1` — passed.
- `cargo clippy --all-targets -- -D warnings` — passed.
- `cargo fmt --check` — passed.

Red test commit: `9fa8d61` (`test: make private redirect SSRF regression reachable`).
