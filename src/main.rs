#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use chrono::{Local, TimeZone};
use eframe::egui;
use reqwest::blocking::{Client, RequestBuilder};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;
use url::Url;

const APP_NAME: &str = "Rust ntfy";
const USER_AGENT: &str = "rust-ntfy-windows/0.1.0";
const MAX_MESSAGES: usize = 250;

fn main() -> eframe::Result {
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([960.0, 720.0])
            .with_min_inner_size([760.0, 560.0]),
        centered: true,
        ..Default::default()
    };

    eframe::run_native(
        APP_NAME,
        native_options,
        Box::new(|creation_context| Ok(Box::new(NtfyApp::new(creation_context)))),
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredSettings {
    server: String,
    topic: String,
    username: String,
    title: String,
    priority: u8,
    tags: String,
    notifications_enabled: bool,
}

impl Default for StoredSettings {
    fn default() -> Self {
        Self {
            server: "https://ntfy.sh".to_owned(),
            topic: String::new(),
            username: String::new(),
            title: String::new(),
            priority: 3,
            tags: String::new(),
            notifications_enabled: true,
        }
    }
}

#[derive(Clone, Debug)]
struct ConnectionSettings {
    server: String,
    topic: String,
    username: String,
    password: String,
}

#[derive(Clone, Debug, Deserialize)]
struct NtfyMessage {
    id: String,
    #[serde(default)]
    time: i64,
    #[serde(default)]
    event: String,
    #[serde(default)]
    topic: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    message: String,
    #[serde(default)]
    priority: Option<u8>,
    #[serde(default)]
    tags: Vec<String>,
}

impl NtfyMessage {
    fn display_title(&self) -> &str {
        self.title.as_deref().unwrap_or("ntfy message")
    }

    fn display_time(&self) -> String {
        Local
            .timestamp_opt(self.time, 0)
            .single()
            .map(|value| value.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| self.time.to_string())
    }
}

enum WorkerEvent {
    Status(String),
    Message(NtfyMessage),
    PublishFinished(Result<String, String>),
    SubscriptionStopped,
}

struct NtfyApp {
    settings: StoredSettings,
    password: String,
    publish_message: String,
    status: String,
    publishing: bool,
    subscribed: bool,
    messages: Vec<NtfyMessage>,
    seen_ids: HashSet<String>,
    worker_tx: Sender<WorkerEvent>,
    worker_rx: Receiver<WorkerEvent>,
    subscription_stop: Option<Arc<AtomicBool>>,
}

impl NtfyApp {
    fn new(_creation_context: &eframe::CreationContext<'_>) -> Self {
        let (worker_tx, worker_rx) = mpsc::channel();
        Self {
            settings: load_settings(),
            password: String::new(),
            publish_message: String::new(),
            status: "Enter a topic, then subscribe or publish.".to_owned(),
            publishing: false,
            subscribed: false,
            messages: Vec::new(),
            seen_ids: HashSet::new(),
            worker_tx,
            worker_rx,
            subscription_stop: None,
        }
    }

    fn connection_settings(&self) -> ConnectionSettings {
        ConnectionSettings {
            server: self.settings.server.trim().to_owned(),
            topic: self.settings.topic.trim().to_owned(),
            username: self.settings.username.trim().to_owned(),
            password: self.password.clone(),
        }
    }

    fn validate_connection(&self) -> Result<ConnectionSettings, String> {
        let settings = self.connection_settings();
        build_topic_url(&settings.server, &settings.topic)?;
        Ok(settings)
    }

    fn start_subscription(&mut self) {
        let settings = match self.validate_connection() {
            Ok(settings) => settings,
            Err(error) => {
                self.status = error;
                return;
            }
        };

        self.stop_subscription();
        let stop = Arc::new(AtomicBool::new(false));
        self.subscription_stop = Some(stop.clone());
        self.subscribed = true;
        self.status = format!("Subscribing to {}...", settings.topic);

        let tx = self.worker_tx.clone();
        thread::spawn(move || run_subscription(settings, stop, tx));
    }

    fn stop_subscription(&mut self) {
        if let Some(stop) = self.subscription_stop.take() {
            stop.store(true, Ordering::Relaxed);
        }
        self.subscribed = false;
    }

    fn publish(&mut self) {
        if self.publish_message.trim().is_empty() {
            self.status = "Message text is required.".to_owned();
            return;
        }

        let settings = match self.validate_connection() {
            Ok(settings) => settings,
            Err(error) => {
                self.status = error;
                return;
            }
        };

        let message = self.publish_message.trim().to_owned();
        let title = self.settings.title.trim().to_owned();
        let tags = self.settings.tags.trim().to_owned();
        let priority = self.settings.priority;
        let tx = self.worker_tx.clone();

        self.publishing = true;
        self.status = "Publishing message...".to_owned();
        thread::spawn(move || {
            let result = publish_message(settings, message, title, tags, priority);
            let _ = tx.send(WorkerEvent::PublishFinished(result));
        });
    }

    fn drain_worker_events(&mut self) {
        while let Ok(event) = self.worker_rx.try_recv() {
            match event {
                WorkerEvent::Status(status) => self.status = status,
                WorkerEvent::Message(message) => {
                    if self.seen_ids.insert(message.id.clone()) {
                        if self.settings.notifications_enabled {
                            show_windows_notification(&message);
                        }
                        self.messages.push(message);
                        if self.messages.len() > MAX_MESSAGES {
                            let removed = self.messages.remove(0);
                            self.seen_ids.remove(&removed.id);
                        }
                    }
                }
                WorkerEvent::PublishFinished(result) => {
                    self.publishing = false;
                    match result {
                        Ok(status) => {
                            self.status = status;
                            self.publish_message.clear();
                        }
                        Err(error) => self.status = error,
                    }
                }
                WorkerEvent::SubscriptionStopped => {
                    self.subscribed = false;
                    self.subscription_stop = None;
                }
            }
        }
    }

    fn render_connection(&mut self, ui: &mut egui::Ui) {
        ui.heading("Connection");
        egui::Grid::new("connection-grid")
            .num_columns(2)
            .spacing([12.0, 8.0])
            .show(ui, |ui| {
                ui.label("Server URL");
                ui.add(
                    egui::TextEdit::singleline(&mut self.settings.server)
                        .hint_text("https://ntfy.sh")
                        .desired_width(f32::INFINITY),
                );
                ui.end_row();

                ui.label("Topic");
                ui.add(
                    egui::TextEdit::singleline(&mut self.settings.topic)
                        .hint_text("my-private-topic")
                        .desired_width(f32::INFINITY),
                );
                ui.end_row();

                ui.label("Username");
                ui.add(
                    egui::TextEdit::singleline(&mut self.settings.username)
                        .hint_text("optional")
                        .desired_width(f32::INFINITY),
                );
                ui.end_row();

                ui.label("Password / token");
                ui.add(
                    egui::TextEdit::singleline(&mut self.password)
                        .password(true)
                        .hint_text("not saved to disk")
                        .desired_width(f32::INFINITY),
                );
                ui.end_row();
            });

        ui.horizontal(|ui| {
            if self.subscribed {
                if ui.button("Stop subscription").clicked() {
                    self.stop_subscription();
                    self.status = "Subscription stopping...".to_owned();
                }
            } else if ui.button("Subscribe").clicked() {
                self.start_subscription();
            }
            ui.checkbox(
                &mut self.settings.notifications_enabled,
                "Show Windows notifications",
            );
        });
    }

    fn render_publisher(&mut self, ui: &mut egui::Ui) {
        ui.heading("Publish");
        egui::Grid::new("publish-grid")
            .num_columns(2)
            .spacing([12.0, 8.0])
            .show(ui, |ui| {
                ui.label("Title");
                ui.add(
                    egui::TextEdit::singleline(&mut self.settings.title)
                        .hint_text("optional")
                        .desired_width(f32::INFINITY),
                );
                ui.end_row();

                ui.label("Priority");
                egui::ComboBox::from_id_salt("priority")
                    .selected_text(priority_label(self.settings.priority))
                    .show_ui(ui, |ui| {
                        for value in 1..=5 {
                            ui.selectable_value(
                                &mut self.settings.priority,
                                value,
                                priority_label(value),
                            );
                        }
                    });
                ui.end_row();

                ui.label("Tags");
                ui.add(
                    egui::TextEdit::singleline(&mut self.settings.tags)
                        .hint_text("warning,computer")
                        .desired_width(f32::INFINITY),
                );
                ui.end_row();
            });

        ui.label("Message");
        ui.add(
            egui::TextEdit::multiline(&mut self.publish_message)
                .hint_text("Type a notification message")
                .desired_rows(4)
                .desired_width(f32::INFINITY),
        );

        let send = ui.add_enabled(!self.publishing, egui::Button::new("Send message"));
        if send.clicked() {
            self.publish();
        }
    }

    fn render_messages(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading("Received messages");
            if ui.small_button("Clear").clicked() {
                self.messages.clear();
                self.seen_ids.clear();
            }
        });

        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                if self.messages.is_empty() {
                    ui.weak("No messages received in this session.");
                    return;
                }

                for message in self.messages.iter().rev() {
                    egui::Frame::group(ui.style()).show(ui, |ui| {
                        ui.horizontal_wrapped(|ui| {
                            ui.label(egui::RichText::new(message.display_title()).strong());
                            ui.weak(message.display_time());
                            if let Some(priority) = message.priority {
                                ui.weak(priority_label(priority));
                            }
                        });
                        ui.label(&message.message);
                        if !message.tags.is_empty() {
                            ui.weak(format!("Tags: {}", message.tags.join(", ")));
                        }
                        ui.weak(format!("Topic: {} · ID: {}", message.topic, message.id));
                    });
                    ui.add_space(6.0);
                }
            });
    }
}

impl eframe::App for NtfyApp {
    fn logic(&mut self, context: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_worker_events();
        context.request_repaint_after(Duration::from_millis(250));
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ui, |ui| {
            ui.heading(APP_NAME);
            ui.label("Subscribe to an ntfy topic and publish messages from a native Windows app.");
            ui.separator();

            egui::CollapsingHeader::new("Server and subscription")
                .default_open(true)
                .show(ui, |ui| self.render_connection(ui));
            ui.add_space(8.0);

            egui::CollapsingHeader::new("Send a notification")
                .default_open(true)
                .show(ui, |ui| self.render_publisher(ui));
            ui.add_space(8.0);

            ui.separator();
            ui.horizontal_wrapped(|ui| {
                ui.label(egui::RichText::new("Status:").strong());
                ui.label(&self.status);
            });
            ui.separator();
            self.render_messages(ui);
        });
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        self.stop_subscription();
        let _ = save_settings(&self.settings);
    }
}

fn run_subscription(settings: ConnectionSettings, stop: Arc<AtomicBool>, tx: Sender<WorkerEvent>) {
    let client = match Client::builder()
        .user_agent(USER_AGENT)
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(35))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            let _ = tx.send(WorkerEvent::Status(format!(
                "Could not create HTTP client: {error}"
            )));
            let _ = tx.send(WorkerEvent::SubscriptionStopped);
            return;
        }
    };

    let mut since = "10m".to_owned();
    let _ = tx.send(WorkerEvent::Status(format!(
        "Subscribed to {}. Polling for messages...",
        settings.topic
    )));

    while !stop.load(Ordering::Relaxed) {
        let url = match build_subscription_url(&settings.server, &settings.topic, &since) {
            Ok(url) => url,
            Err(error) => {
                let _ = tx.send(WorkerEvent::Status(error));
                break;
            }
        };

        let request = apply_auth(client.get(url), &settings);
        match request.send() {
            Ok(response) => {
                if !response.status().is_success() {
                    let status = response.status();
                    let body = response.text().unwrap_or_default();
                    let _ = tx.send(WorkerEvent::Status(format!(
                        "Subscription request failed ({status}): {}",
                        compact_error_body(&body)
                    )));
                } else {
                    match response.text() {
                        Ok(body) => {
                            for line in body.lines().filter(|line| !line.trim().is_empty()) {
                                match serde_json::from_str::<NtfyMessage>(line) {
                                    Ok(message) if message.event == "message" => {
                                        since = message.id.clone();
                                        let _ = tx.send(WorkerEvent::Message(message));
                                    }
                                    Ok(_) => {}
                                    Err(error) => {
                                        let _ = tx.send(WorkerEvent::Status(format!(
                                            "Ignored an invalid server response: {error}"
                                        )));
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            let _ = tx.send(WorkerEvent::Status(format!(
                                "Could not read subscription response: {error}"
                            )));
                        }
                    }
                }
            }
            Err(error) => {
                let _ = tx.send(WorkerEvent::Status(format!(
                    "Subscription connection error: {error}"
                )));
            }
        }

        sleep_until_next_poll(&stop);
    }

    let _ = tx.send(WorkerEvent::SubscriptionStopped);
}

fn publish_message(
    settings: ConnectionSettings,
    message: String,
    title: String,
    tags: String,
    priority: u8,
) -> Result<String, String> {
    let url = build_topic_url(&settings.server, &settings.topic)?;
    let client = Client::builder()
        .user_agent(USER_AGENT)
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| format!("Could not create HTTP client: {error}"))?;

    let mut request = client.post(url).body(message);
    if !title.is_empty() {
        request = request.header("Title", title);
    }
    if !tags.is_empty() {
        request = request.header("Tags", tags);
    }
    request = request.header("Priority", priority.to_string());
    request = apply_auth(request, &settings);

    let response = request
        .send()
        .map_err(|error| format!("Publish request failed: {error}"))?;
    let status = response.status();
    if status.is_success() {
        Ok(format!("Message published successfully ({status})."))
    } else {
        let body = response.text().unwrap_or_default();
        Err(format!(
            "Publish failed ({status}): {}",
            compact_error_body(&body)
        ))
    }
}

fn apply_auth(request: RequestBuilder, settings: &ConnectionSettings) -> RequestBuilder {
    if settings.username.is_empty() {
        request
    } else {
        request.basic_auth(&settings.username, Some(&settings.password))
    }
}

fn build_topic_url(server: &str, topic: &str) -> Result<Url, String> {
    let topic = topic.trim();
    if topic.is_empty() {
        return Err("Topic is required.".to_owned());
    }

    let normalized_server = normalize_server(server);
    let mut url =
        Url::parse(&normalized_server).map_err(|error| format!("Invalid server URL: {error}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("Server URL must use http:// or https://.".to_owned());
    }

    url.set_query(None);
    url.set_fragment(None);
    url.path_segments_mut()
        .map_err(|_| "Server URL cannot be used as a base URL.".to_owned())?
        .pop_if_empty()
        .push(topic);
    Ok(url)
}

fn build_subscription_url(server: &str, topic: &str, since: &str) -> Result<Url, String> {
    let mut url = build_topic_url(server, topic)?;
    url.path_segments_mut()
        .map_err(|_| "Server URL cannot be used as a base URL.".to_owned())?
        .push("json");
    url.query_pairs_mut()
        .append_pair("poll", "1")
        .append_pair("since", since);
    Ok(url)
}

fn normalize_server(server: &str) -> String {
    let server = server.trim().trim_end_matches('/');
    if server.contains("://") {
        server.to_owned()
    } else {
        format!("https://{server}")
    }
}

fn compact_error_body(body: &str) -> String {
    let compact = body.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.is_empty() {
        "no response body".to_owned()
    } else {
        compact.chars().take(240).collect()
    }
}

fn sleep_until_next_poll(stop: &AtomicBool) {
    for _ in 0..20 {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        thread::sleep(Duration::from_millis(250));
    }
}

fn priority_label(priority: u8) -> &'static str {
    match priority {
        1 => "1 - minimum",
        2 => "2 - low",
        4 => "4 - high",
        5 => "5 - maximum",
        _ => "3 - default",
    }
}

fn settings_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("RustNtfy")
        .join("settings.json")
}

fn load_settings() -> StoredSettings {
    let path = settings_path();
    fs::read_to_string(path)
        .ok()
        .and_then(|contents| serde_json::from_str(&contents).ok())
        .unwrap_or_default()
}

fn save_settings(settings: &StoredSettings) -> Result<(), String> {
    let path = settings_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("Could not create settings directory: {error}"))?;
    }
    let contents = serde_json::to_string_pretty(settings)
        .map_err(|error| format!("Could not encode settings: {error}"))?;
    fs::write(path, contents).map_err(|error| format!("Could not save settings: {error}"))
}

#[cfg(target_os = "windows")]
fn show_windows_notification(message: &NtfyMessage) {
    use tauri_winrt_notification::{Duration as ToastDuration, Sound, Toast};

    let _ = Toast::new(Toast::POWERSHELL_APP_ID)
        .title(message.display_title())
        .text1(&message.message)
        .sound(Some(Sound::Default))
        .duration(ToastDuration::Short)
        .show();
}

#[cfg(not(target_os = "windows"))]
fn show_windows_notification(_message: &NtfyMessage) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_publish_url_and_escapes_topic() {
        let url = build_topic_url("ntfy.sh/", "alerts office").expect("valid URL");
        assert_eq!(url.as_str(), "https://ntfy.sh/alerts%20office");
    }

    #[test]
    fn builds_polling_subscription_url() {
        let url = build_subscription_url("https://example.test/base", "alerts", "abc123")
            .expect("valid URL");
        assert_eq!(
            url.as_str(),
            "https://example.test/base/alerts/json?poll=1&since=abc123"
        );
    }

    #[test]
    fn rejects_empty_topic() {
        assert_eq!(
            build_topic_url("https://ntfy.sh", " ").expect_err("topic should be required"),
            "Topic is required."
        );
    }

    #[test]
    fn parses_ntfy_message_event() {
        let message: NtfyMessage = serde_json::from_str(
            r#"{"id":"abc123","time":1700000000,"event":"message","topic":"alerts","message":"Build finished","priority":4,"tags":["white_check_mark"]}"#,
        )
        .expect("message should parse");
        assert_eq!(message.id, "abc123");
        assert_eq!(message.message, "Build finished");
        assert_eq!(message.priority, Some(4));
    }
}
