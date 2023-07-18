use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;

use chrono::Utc;
use slint::Weak;

use katniss_test::protos::spacecorp::JumpDriveStatus;

slint::include_modules!();

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
            let mode = status.lock().unwrap().mode.clone();

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

            update_jumpdrive_temperature(status.clone());
        }
    }
}

fn update_jumpdrive_temperature(status: Arc<Mutex<JumpDriveStatus>>) {
    dbg!(status.lock().unwrap().temperature);
    let powered_time_secs = status.lock().unwrap().powered_time_secs;

    // fake logarithm
    status.lock().unwrap().temperature = powered_time_secs * 3;
}

async fn update_gui(weak_window: Weak<MainWindow>, status: Arc<Mutex<JumpDriveStatus>>) {
    loop {
        let mode: i32 = status.lock().unwrap().mode.clone();
        let powered_time_secs = status.lock().unwrap().powered_time_secs.clone();
        let temperature = status.lock().unwrap().temperature.clone();
        weak_window
            .upgrade_in_event_loop(move |window| {
                window.set_powered_time_secs(powered_time_secs);
                window.set_temperature(temperature);
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

    window.on_power_clicked(move || {
        let control_status = status.clone();
        start_jumpdrive(control_status);
    });

    window.run().unwrap();

    Ok(())
}
