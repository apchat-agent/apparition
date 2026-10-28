# Lane report av18

Date: 2026-10-29

## Delivered

- OAuth now requests the feed RPC permissions needed for saved feeds and feed skeletons. A stored session without those grants receives a friendly feed reauthorization page. PDS XRPC failures log safe status and error details at WARN without logging credentials.
- The web UI can create indexed replies with correct `reply.root` and `reply.parent` strong references. Thread pages show the available parent chain and indexed direct replies, and timeline reply context remains useful when the parent post is missing.
- The authenticated UI now has a responsive three-column desktop layout and a single-column phone layout, pinned feed tabs, inline outline icons, and post action links and counts. The existing teal accent and CSP are retained.
- README documents the features and explains that existing sessions must sign in again to allow feeds. No live server was accessed and nothing was pushed.

## OAuth scope

Exact scope currently published in OAuth client metadata and requested during authorization:

```text
atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete rpc:app.bsky.actor.getPreferences?aud=* rpc:app.bsky.feed.getFeedSkeleton?aud=*
```

Existing sessions with the previous scope lack these feed RPC permissions. They must sign in again to allow feeds. The app requests `aud=*` on these individual RPC permissions; service-auth requests use the concrete AppView DID as `aud` and the feed skeleton method as `lxm`.

## Red runs

These focused checks failed against the preceding implementation before their corresponding green commits:

- `cargo test --locked --offline --test m6_oauth confidential_client_metadata_is_published` and `cargo test --locked --offline --test m6_oauth old_scope_session_gets_a_friendly_feed_reauthorization_page`: metadata omitted both feed RPC permissions, and old sessions did not get the reauthorization page.
- `cargo test --locked --offline --test m6_oauth_protocol pds_4xx_logs_safe_error_details_without_credentials`: the PDS error code and message were swallowed instead of logged.
- `cargo test --locked --offline --test m6_oauth reply_form_checks_origin_and_writes_through_to_the_index` and `cargo test --locked --offline --test m6_oauth web_thread_renders_the_parent_chain_and_nested_replies_in_time_order`: reply route/form behavior and the thread composer/rendering were missing.
- `cargo test --locked --offline --test m6_oauth_protocol fake_pds_receives_correct_reply_root_and_parent_strong_refs`: fake PDS did not receive the required root and parent references for both reply cases.
- `cargo test --locked --offline --test m6_oauth bsky_style_shell_renders_navigation_pinned_feed_tabs_and_post_actions`: the requested three-column navigation, feed tabs, and action row were absent.
- `cargo test --locked --offline --test m6_oauth timeline_reply_context_uses_verified_parent_handle_when_post_is_missing`: replies lost their parent author context when the parent post was unavailable.

## Commits

1. `3f731e5` red: require feed permissions and old session page
2. `bcf0439` green: request and track feed RPC permissions
3. `5ca6820` red: capture safe PDS error details
4. `2ac6e27` green: log safe PDS XRPC failure details
5. `a164e36` red: cover reply creation and thread composer
6. `8448dd1` green: create indexed replies and render threads
7. `e42ef57` red: require three-column social feed layout
8. `a246a16` green: build responsive three-column feed layout
9. `620346f` red: retain reply context for missing parent posts
10. `5f44356` green: show context for replies with missing parents
11. `040ad91` test: verify production feed reauthorization
12. Final documentation commit: README and this report.

## Verification

- `cargo test --locked --offline`: passed on the initial full run.
- `cargo test --locked --offline -- --test-threads=1`: passed, 172 passed, 2 ignored, 0 failed.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --locked --offline -- -D warnings`: passed.
- `cargo deny check licenses`: passed (`licenses ok`).
- One repeated parallel full-suite run had a transient failure in the `snapshot_lock` test binary. The binary passed when rerun alone, and the full serialized suite passed.
