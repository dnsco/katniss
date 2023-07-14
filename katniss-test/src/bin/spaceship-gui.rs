use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;

use chrono::Utc;
use slint::Weak;

use katniss_test::protos::spacecorp::{JumpDriveMode, JumpDriveStatus};

fn start_jumpdrive(status: Arc<Mutex<JumpDriveStatus>>) {
    let mut status = status.lock().unwrap();

    match status.mode {
        0 => status.mode = 1,
        _ => {
            status.mode = 0;
            status.powered_time_secs = 0;
        }
    }
}

async fn jumpdrive_tick(status: Arc<Mutex<JumpDriveStatus>>) {
    let mut tick_start = Utc::now();
    loop {
        let now = Utc::now();
        if now - tick_start > chrono::Duration::seconds(1) {
            tick_start = now;

            // accessing the arc mutex directly in match locks up the threads
            let mode = status.lock().unwrap().mode;

            match mode {
                1 => {
                    status.lock().unwrap().powered_time_secs += 1;
                    if status.lock().unwrap().powered_time_secs > 5 {
                        status.lock().unwrap().mode = 2;
                    }
                }
                2 => status.lock().unwrap().powered_time_secs += 1,
                _ => {}
            }
        }
    }
}

async fn update_gui(weak_window: Weak<MainWindow>, status: Arc<Mutex<JumpDriveStatus>>) {
    loop {
        let mode: i32 = status.lock().unwrap().mode.clone();
        let powered_time_secs = status.lock().unwrap().powered_time_secs.clone();
        weak_window
            .upgrade_in_event_loop(move |window| {
                window.set_powered_time_secs(powered_time_secs);
                window.set_mode(mode);
            })
            .unwrap();
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let status = Arc::new(Mutex::new(JumpDriveStatus::default()));
    let window = MainWindow::new().unwrap();

    let weak_window: Weak<MainWindow> = window.as_weak();
    let gui_status = status.clone();
    let sim_status = status.clone();

    thread::spawn(move || {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(update_gui(weak_window, gui_status))
    });

    thread::spawn(move || {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(jumpdrive_tick(sim_status))
    });

    window.on_clicked(move || {
        let control_status = status.clone();
        start_jumpdrive(control_status);
    });
    window.run().unwrap();

    Ok(())
}

slint::slint! {
    import { VerticalBox , HorizontalBox} from "std-widgets.slint";
export component MainWindow inherits Window {
        width: 800px;
        height: 600px;
        callback clicked;

        in property <int> powered_time_secs;
        in property <int> mode;

        VerticalBox {
            Text {
                text: "Jumpdrive Status";
                font-size: 24px;
            }

            HorizontalBox {
                Text {
                        text: powered_time_secs;
                        font-size: 24px;
                    }
            }

            HorizontalBox {
                Rectangle {
                    background: black;
                    callback clicked;

                    width: 96px;
                    height: 96px;
                    border-radius: self.width/2;

                    Text {
                        text: "Power";
                    }
                    TouchArea {
                        clicked => {
                            // pretty sure "root" refers to the root of the current slint "file",
                            // not necessarily the outermost element.
                            root.clicked();
                        }
                    }
                }
                Rectangle {
                    animate background {
                    duration: 500ms;
                    }

                    states [
                            off when mode == 0: {
                                background: grey;
                            }
                            warming when mode == 1: {
                                background: yellow;
                            }
                            engaged when mode >= 2: {
                                background: green;
                            }
                        ]
                    Rectangle {
                        background: black;
                        height: 48px;
                        width: 128px;

                        Text {
                            text: mode;
                            font-size: 24px;

                            states [
                                off when mode == 0: {
                                    text: "Hyperdrive Offline";
                                }
                                warming when mode == 1: {
                                    text: "Hyperdrive Warming";
                                }
                                engaged when mode >= 2: {
                                    text: "Hyperdrive Engaged";
                                }
                            ]

                        }
                    }
                }
            }
        }
    }
}
