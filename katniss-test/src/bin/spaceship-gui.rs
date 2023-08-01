use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;

use slint::Weak;

use katniss_test::protos::spacecorp::JumpDriveStatus;

slint::include_modules!();

// TODO!
// #1
// change temperature concept to have a desired and actual values
// dig up the old power_times_sec increment code and re-purpose to
// simulate closing the delta between actual and desired
//
// #2
//  Figure out why the gui launches with wrong scaling and corrects
//
// #3
//  See what it takes to make gui scale to different display dimensions
//
// #4
//  Extract panel/quadrant from main window so we can see what it's
// like to more than one
//
// #5
// See about making a knob sim, slider works ok for demo though.

const TEMPERATURE_CEILING: i32 = 1000;
const WINDOW_WIDTH: i32 = 540;
const WINDOW_HEIGHT: i32 = 960;

fn set_jumpdrive_temperature(status: Arc<Mutex<JumpDriveStatus>>, new_temp: i32) {
    let mut status = status.lock().unwrap();
    status.temperature = new_temp;
}

async fn gui_tick(weak_window: Weak<MainWindow>, status: Arc<Mutex<JumpDriveStatus>>) {
    loop {
        let temperature = status.lock().unwrap().temperature.clone();
        weak_window
            .upgrade_in_event_loop(move |window| {
                window.set_temperature(temperature);
            })
            .unwrap();
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let status = Arc::new(Mutex::new(JumpDriveStatus::default()));
    let window = MainWindow::new().unwrap();

    let weak_window: Weak<MainWindow> = window.as_weak();
    let gui_status = status.clone();
    let slider_control_status = status.clone();

    window.set_window_width(WINDOW_WIDTH);
    window.set_window_height(WINDOW_HEIGHT);
    window.set_temperature_ceiling(TEMPERATURE_CEILING);

    thread::spawn(move || {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(gui_tick(weak_window, gui_status))
    });

    window.on_temperature_changed(move |change| {
        dbg!(change);
        set_jumpdrive_temperature(slider_control_status.clone(), change as i32);
    });

    window.run().unwrap();

    Ok(())
}
