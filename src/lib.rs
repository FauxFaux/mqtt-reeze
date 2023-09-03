use std::env;
use std::str::FromStr;
use std::time::Duration;

pub use rumqttc::{MqttOptions, QoS};

use anyhow::{anyhow, Context, Result};
use log::{info, trace, warn};
use rumqttc::ConnectionError::MqttState;
use rumqttc::{Event, EventLoop, Incoming, Outgoing, StateError};
use serde::Serialize;
use tokio::time::timeout;

pub struct Mqtt {
    client: rumqttc::AsyncClient,
    loop_handle: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    name: String,
    qos: QoS,
    retain: bool,
}

impl Topic {
    pub fn new(name: impl AsRef<str>, qos: QoS, retain: bool) -> Self {
        Self {
            name: name.as_ref().to_owned(),
            qos,
            retain,
        }
    }
}

impl Mqtt {
    pub fn new(opts: MqttOptions, buffer_size: usize) -> Result<Self> {
        let (client, loop_handle) = rumqttc::AsyncClient::new(opts, buffer_size);
        let loop_handle = tokio::spawn(log_for_loop(loop_handle));
        Ok(Self {
            client,
            loop_handle,
        })
    }

    pub fn new_from_env(id: &str) -> Result<Self> {
        let host = env_var("MQTT_HOST")?;
        let port = env_parse_or("MQTT_PORT", 1883)?;
        let keep_alive = env_parse_or("MQTT_KEEP_ALIVE_SECS", 15)?;
        let mut mqtt_opts = MqttOptions::new(id, host, port);
        mqtt_opts.set_keep_alive(Duration::from_secs(keep_alive));
        Self::new(mqtt_opts, 1024)
    }

    pub async fn publish_json(&mut self, topic: &Topic, payload: impl Serialize) -> Result<()> {
        self.client
            .publish(&topic.name, topic.qos, topic.retain, serde_json::to_vec(&payload)?)
            .await
            .context("publishing to mqtt")
    }

    pub async fn publish(&mut self, topic: &Topic, payload: impl Into<Vec<u8>>) -> Result<()> {
        self.client
            .publish(&topic.name, topic.qos, topic.retain, payload)
            .await
            .context("publishing to mqtt")
    }

    pub async fn finish(self) -> Result<()> {
        // I *believe* this should queue behind real data but have not tried it.
        info!("attempting to initiate a disconnect from the broker...");
        self.client
            .try_disconnect()
            .context("asking mqtt to disconnect")?;

        info!("waiting for broker disconnect...");
        timeout(Duration::from_secs(60), self.loop_handle)
            .await
            .context("waiting for mqtt to flush")?
            .context("joining mqtt")?;

        Ok(())
    }
}

fn env_var(name: &'static str) -> Result<String> {
    env::var(name).with_context(|| anyhow!("reading env var {name:?}"))
}

fn env_parse_or<T: FromStr>(name: &'static str, default: T) -> Result<T>
where
    <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    match env_var(name) {
        Ok(val) => val
            .parse()
            .with_context(|| anyhow!("parsing env var {name:?} as a number")),
        Err(_) => Ok(default),
    }
}

async fn log_for_loop(mut ev: EventLoop) {
    #[derive(Copy, Clone, PartialEq)]
    enum State {
        Unknown,
        AckPrinted,
        ErrPrinted,
    }

    let mut disconnecting = false;
    let mut state = State::Unknown;
    loop {
        let res = ev.poll().await;
        trace!("mqtt event: {:?}", res);
        match res {
            Ok(Event::Incoming(Incoming::PubAck(_))) if state != State::AckPrinted => {
                info!("mqtt broker acknowledged a publication (we're all good)");
                state = State::AckPrinted;
            }
            Ok(Event::Outgoing(Outgoing::Disconnect)) if !disconnecting => {
                info!("asked mqtt broker to disconnect us");
                disconnecting = true;
            }
            Ok(Event::Incoming(Incoming::Disconnect)) if disconnecting => break,
            // mosquitto appears to just drop the connection, generating a:
            // MqttState(Io(Custom { kind: ConnectionAborted, error: "connection closed by peer" }))
            Err(MqttState(StateError::Io(e))) if disconnecting => {
                warn!("connection error during broker disconnect, assuming intention was to disconnect: {e:?}");
                break;
            }
            Ok(some) if state == State::ErrPrinted => {
                warn!(
                    "mqtt client recovered from error: {:?} (hopefully we'll flush)",
                    some
                );
                state = State::Unknown;
            }
            Err(e) if state != State::ErrPrinted => {
                warn!(
                    "mqtt client in an error state (unable to reach broker?): {:?}",
                    e
                );
                state = State::ErrPrinted;
            }
            _ => (),
        }
    }
}
