use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavedFeedPreference {
    pub id: String,
    pub feed_type: String,
    pub value: String,
    pub pinned: bool,
}

pub fn parse_saved_feed_preferences(response: &Value) -> Vec<SavedFeedPreference> {
    let Some(preferences) = response.get("preferences").and_then(Value::as_array) else {
        return Vec::new();
    };

    if let Some(v2) = preferences.iter().find(|preference| {
        preference.get("$type").and_then(Value::as_str)
            == Some("app.bsky.actor.defs#savedFeedsPrefV2")
    }) {
        return v2
            .get("items")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|item| {
                Some(SavedFeedPreference {
                    id: item.get("id")?.as_str()?.to_owned(),
                    feed_type: item.get("type")?.as_str()?.to_owned(),
                    value: item.get("value")?.as_str()?.to_owned(),
                    pinned: item.get("pinned")?.as_bool()?,
                })
            })
            .collect();
    }

    let Some(legacy) = preferences.iter().find(|preference| {
        preference.get("$type").and_then(Value::as_str)
            == Some("app.bsky.actor.defs#savedFeedsPref")
    }) else {
        return Vec::new();
    };
    let pinned = legacy
        .get("pinned")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    let saved = legacy
        .get("saved")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    let mut result = Vec::new();
    for (value, is_pinned) in pinned
        .iter()
        .map(|value| (*value, true))
        .chain(saved.iter().map(|value| (*value, false)))
    {
        if result.iter().any(|item: &SavedFeedPreference| item.value == value) {
            continue;
        }
        result.push(SavedFeedPreference {
            id: value.to_owned(),
            feed_type: "feed".to_owned(),
            value: value.to_owned(),
            pinned: is_pinned,
        });
    }
    result
}

#[cfg(test)]
mod tests {
    use super::parse_saved_feed_preferences;
    use serde_json::json;

    #[test]
    fn saved_feed_preferences_keep_v2_items_and_prefer_v2_over_legacy() {
        let response = json!({
            "preferences": [
                {
                    "$type": "app.bsky.actor.defs#savedFeedsPref",
                    "pinned": ["at://did:plc:legacy/app.bsky.feed.generator/old"],
                    "saved": ["at://did:plc:legacy/app.bsky.feed.generator/old"]
                },
                {
                    "$type": "app.bsky.actor.defs#savedFeedsPrefV2",
                    "items": [
                        {"id":"1", "type":"feed", "value":"at://did:plc:one/app.bsky.feed.generator/a", "pinned":false},
                        {"id":"2", "type":"timeline", "value":"following", "pinned":true},
                        {"id":"3", "type":"feed", "value":"at://did:plc:two/app.bsky.feed.generator/b", "pinned":true},
                        {"id":"4", "type":"list", "value":"at://did:plc:list/app.bsky.graph.list/x", "pinned":true},
                        {"id":"5", "type":"futureType", "value":"opaque", "pinned":false}
                    ]
                }
            ]
        });

        let parsed = parse_saved_feed_preferences(&response);
        assert_eq!(parsed.len(), 5);
        assert_eq!(parsed[1].feed_type, "timeline");
        assert_eq!(parsed[4].feed_type, "futureType");
        let pinned_feeds = parsed
            .iter()
            .filter(|item| item.feed_type == "feed" && item.pinned)
            .map(|item| item.value.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            pinned_feeds,
            ["at://did:plc:two/app.bsky.feed.generator/b"]
        );
        assert!(!parsed.iter().any(|item| item.value.ends_with("/old")));
    }

    #[test]
    fn legacy_saved_feed_preferences_are_used_when_v2_is_absent() {
        let response = json!({
            "preferences": [{
                "$type": "app.bsky.actor.defs#savedFeedsPref",
                "pinned": ["at://did:plc:one/app.bsky.feed.generator/pinned"],
                "saved": [
                    "at://did:plc:one/app.bsky.feed.generator/pinned",
                    "at://did:plc:two/app.bsky.feed.generator/other"
                ]
            }]
        });

        let parsed = parse_saved_feed_preferences(&response);
        assert_eq!(parsed.len(), 2);
        assert!(parsed[0].pinned);
        assert!(!parsed[1].pinned);
    }
}
