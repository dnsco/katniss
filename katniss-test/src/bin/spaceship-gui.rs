use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use katniss_test::protos::spacecorp::JumpDriveStatus;

fn main() -> Result<(), Box<dyn Error>> {
    let status = Arc::new(Mutex::new(JumpDriveStatus::default()));

    let sim_status = status.clone();
    thread::spawn(move || loop {
        let mut status = sim_status.lock().unwrap();
        status.powered_time_secs += 1;
        dbg!(status.powered_time_secs);
        //drops here
        thread::sleep(Duration::from_secs(1));
    });

    let gui_status = status.clone();
    thread::spawn(move || dbg!(gui_status.lock().unwrap().powered_time_secs));

    thread::sleep(Duration::from_secs(3));
    dbg!(status.lock().unwrap().powered_time_secs);
    /// do slint stuff here
    Ok(())
}
