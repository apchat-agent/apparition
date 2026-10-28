use appview_rs::config::Config;

#[test]
fn appview_did_is_configurable_for_did_web_publication() {
    let config =
        Config::from_toml_and_env("", [("APPVIEW_DID", "did:web:appview.example.test")]).unwrap();
    assert_eq!(config.appview_did, "did:web:appview.example.test");
}

#[test]
fn backfill_record_rate_defaults_to_300_and_can_be_configured() {
    let default = Config::from_toml_and_env("", std::iter::empty::<(&str, &str)>()).unwrap();
    assert!(format!("{default:?}").contains("backfill_records_per_second: 300"));

    let configured =
        Config::from_toml_and_env("", [("APPVIEW_BACKFILL_RECORDS_PER_SECOND", "120")]).unwrap();
    assert!(format!("{configured:?}").contains("backfill_records_per_second: 120"));
}

#[test]
fn backfill_record_rate_rejects_zero_and_non_numbers() {
    for value in ["0", "many"] {
        assert!(
            Config::from_toml_and_env("", [("APPVIEW_BACKFILL_RECORDS_PER_SECOND", value)])
                .is_err(),
            "invalid backfill rate {value:?} was accepted"
        );
    }
}

#[test]
fn repository_backfill_is_disabled_by_default_and_requires_explicit_enablement() {
    let default = Config::from_toml_and_env("", std::iter::empty::<(&str, &str)>()).unwrap();
    assert!(format!("{default:?}").contains("backfill_enabled: false"));

    let enabled = Config::from_toml_and_env("", [("APPVIEW_BACKFILL_ENABLED", "true")]).unwrap();
    assert!(format!("{enabled:?}").contains("backfill_enabled: true"));
}

#[test]
fn loopback_oauth_mode_can_be_selected_and_defaults_to_confidential() {
    let default = Config::from_toml_and_env("", std::iter::empty::<(&str, &str)>()).unwrap();
    assert!(format!("{default:?}").contains("oauth_mode: Confidential"));

    let config = Config::from_toml_and_env("", [("APPVIEW_OAUTH_MODE", "loopback")]).unwrap();
    assert!(format!("{config:?}").contains("oauth_mode: Loopback"));

    let config = Config::from_toml_and_env(
        "oauth_mode = \"loopback\"",
        std::iter::empty::<(&str, &str)>(),
    )
    .unwrap();
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

#[test]
fn server_exits_before_starting_on_a_public_address_in_loopback_mode() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_appview-rs"))
        .env_remove("APPVIEW_CONFIG")
        .env("APPVIEW_OAUTH_MODE", "loopback")
        .env("APPVIEW_LISTEN", "0.0.0.0:3000")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("UnsafeLoopbackListen"), "{stderr}");
}
