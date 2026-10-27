use appview_rs::config::Config;

#[test]
fn appview_did_is_configurable_for_did_web_publication() {
    let config =
        Config::from_toml_and_env("", [("APPVIEW_DID", "did:web:appview.example.test")]).unwrap();
    assert_eq!(config.appview_did, "did:web:appview.example.test");
}
