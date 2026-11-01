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
fn stale_jetstream_cursor_threshold_defaults_to_ten_minutes_and_is_configurable() {
    let default = Config::from_toml_and_env("", std::iter::empty::<(&str, &str)>()).unwrap();
    assert!(format!("{default:?}").contains("jetstream_stale_cursor_threshold_secs: 600"));

    let configured = Config::from_toml_and_env(
        "jetstream_stale_cursor_threshold_secs = 120",
        [("APPVIEW_JETSTREAM_STALE_CURSOR_THRESHOLD_SECS", "300")],
    )
    .unwrap();
    assert!(format!("{configured:?}").contains("jetstream_stale_cursor_threshold_secs: 300"));

    let from_toml = Config::from_toml_and_env(
        "jetstream_stale_cursor_threshold_secs = 120",
        std::iter::empty::<(&str, &str)>(),
    )
    .unwrap();
    assert!(format!("{from_toml:?}").contains("jetstream_stale_cursor_threshold_secs: 120"));
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
fn backfill_max_age_days_defaults_to_thirty_and_is_configurable() {
    let default = Config::from_toml_and_env("", std::iter::empty::<(&str, &str)>()).unwrap();
    assert!(
        format!("{default:?}").contains("backfill_max_age_days: 30"),
        "backfill keeps only 30 days of post/like/repost history by default"
    );

    let from_env =
        Config::from_toml_and_env("", [("APPVIEW_BACKFILL_MAX_AGE_DAYS", "7")]).unwrap();
    assert!(format!("{from_env:?}").contains("backfill_max_age_days: 7"));

    let from_toml =
        Config::from_toml_and_env("backfill_max_age_days = 14", std::iter::empty::<(&str, &str)>())
            .unwrap();
    assert!(format!("{from_toml:?}").contains("backfill_max_age_days: 14"));

    let env_over_toml = Config::from_toml_and_env(
        "backfill_max_age_days = 14",
        [("APPVIEW_BACKFILL_MAX_AGE_DAYS", "7")],
    )
    .unwrap();
    assert!(format!("{env_over_toml:?}").contains("backfill_max_age_days: 7"));
}

#[test]
fn backfill_max_age_days_accepts_zero_and_rejects_garbage() {
    let unlimited = Config::from_toml_and_env("", [("APPVIEW_BACKFILL_MAX_AGE_DAYS", "0")]).unwrap();
    assert!(
        format!("{unlimited:?}").contains("backfill_max_age_days: 0"),
        "0 means no window: index the whole repository as before"
    );

    for value in ["many", "-1", "1.5"] {
        let error = Config::from_toml_and_env("", [("APPVIEW_BACKFILL_MAX_AGE_DAYS", value)])
            .expect_err("garbage max age must be rejected");
        assert!(
            format!("{error}").contains("APPVIEW_BACKFILL_MAX_AGE_DAYS"),
            "the error for {value:?} must name the variable: {error}"
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

#[test]
fn login_allowlist_is_disabled_by_default_and_can_be_configured() {
    let default = Config::from_toml_and_env("", std::iter::empty::<(&str, &str)>()).unwrap();
    assert!(format!("{default:?}").contains("login_allowlist: []"));

    let from_toml = Config::from_toml_and_env(
        "login_allowlist = [\"did:plc:alice\", \"alice.example\"]",
        std::iter::empty::<(&str, &str)>(),
    )
    .unwrap();
    assert!(
        format!("{from_toml:?}")
            .contains("login_allowlist: [\"did:plc:alice\", \"alice.example\"]")
    );

    let from_env = Config::from_toml_and_env(
        "",
        [("APPVIEW_LOGIN_ALLOWLIST", "did:plc:alice, alice.example")],
    )
    .unwrap();
    assert!(
        format!("{from_env:?}").contains("login_allowlist: [\"did:plc:alice\", \"alice.example\"]")
    );
}
