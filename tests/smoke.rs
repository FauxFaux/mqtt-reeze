use mqtt_reeze::Mqtt;
use mqtt_reeze::MqttOptions;
use mqtt_reeze::Topic;
use ntest::timeout;
use serde_json::json;

// requires a local MQTT broker (i.e., sudo apt install mosquitto).
#[tokio::test]
#[timeout(1_000)]
async fn smoke_test_mqtt_cycle() -> anyhow::Result<()> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    let mqtt_opts = MqttOptions::new("test_client", "localhost", 1883);
    let mqtt = Mqtt::new(mqtt_opts, 1024)?;
    let topic = Topic::new("smoke/test", rumqttc::QoS::AtLeastOnce, true);

    mqtt.publish_json(&topic, &json!({
        "status": "ok",
        "test_run": 1
    })).await?;

    mqtt.finish().await?;

    Ok(())
}
