use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;

use chrono::Utc;
use slint::Weak;

use katniss_test::protos::spacecorp::JumpDriveStatus;

async fn run_jumpdrive(status: Arc<Mutex<JumpDriveStatus>>) {
    let mut start = Utc::now();
    loop {
        let now = Utc::now();
        if (now - start) > chrono::Duration::seconds(1) {
            start = Utc::now();
            let mut status = status.lock().unwrap();
            status.powered_time_secs += 1;
            dbg!(status.powered_time_secs);
        }
    }
}

async fn update_gui(weak_window: Weak<MainWindow>, status: Arc<Mutex<JumpDriveStatus>>) {
    loop {
        let powered_time_secs = status.lock().unwrap().powered_time_secs.clone();
        weak_window
            .upgrade_in_event_loop(move |window| {
                window.set_powered_time_secs(powered_time_secs);
            })
            .unwrap();
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let status = Arc::new(Mutex::new(JumpDriveStatus::default()));
    let window = MainWindow::new().unwrap();
    window.set_powered_time_secs(status.lock().unwrap().powered_time_secs);

    let gui_status = status.clone();
    let weak_window = window.as_weak();
    thread::spawn(move || {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(update_gui(weak_window, gui_status))
    });

    let sim_status = status.clone();
    thread::spawn(move || {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(run_jumpdrive(sim_status))
    });

    window.run().unwrap();

    Ok(())
}

slint::slint! {
    export component MainWindow inherits Window {
        width: 800px;
        height: 600px;

        in property <int> powered_time_secs;

        Text {
            text: "Jumpdrive Status";
            font-size: 24px;
            y: 12px;

            Text {
                text: powered_time_secs;
                font-size: 24px;
                // manually sizing this space for now, must we do all
                // positioning math are there alignment concepts like
                // in swiftui/jetpack (vstack, hstack, etc)?
                y: 36px;
            }
        }
    }
}
