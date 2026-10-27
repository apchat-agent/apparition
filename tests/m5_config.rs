use appview_rs::config::Config;

#[test]
fn appview_did_is_configurable_for_did_web_publication() {
    let config =
        Config::from_toml_and_env("", [("APPVIEW_DID", "did:web:appview.example.test")]).unwrap();
    assert_eq!(config.appview_did, "did:web:appview.example.test");
}

#[test]
fn loopback_oauth_mode_can_be_selected_and_defaults_to_confidential() {
    let default = Config::from_toml_and_env("", std::iter::empty::<(&str, &str)>()).unwrap();
    assert!(format!("{default:?}").contains("oauth_mode: Confidential"));

    let config = Config::from_toml_and_env("", [("APPVIEW_OAUTH_MODE", "loopback")]).unwrap();
    assert!(format!("{config:?}").contains("oauth_mode: Loopback"));
}

#[test]
fn loopback_oauth_refuses_non_loopback_listen_addresses() {
    let error = Config::from_toml_and_env(
        "",
        [
            ("APPVIEW_OAUTH_MODE", "loopback"),
            ("APPVIEW_LISTEN", "0.0.0.0:3000"),
        ],
    );
    assert!(error.is_err(), "loopback OAuth must not bind publicly");
}

#[test]
fn unknown_oauth_mode_is_rejected() {
    let error = Config::from_toml_and_env("", [("APPVIEW_OAUTH_MODE", "public")]);
    assert!(error.is_err(), "unknown OAuth modes must fail closed");
}
