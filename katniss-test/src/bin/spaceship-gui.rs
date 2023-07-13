use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use katniss_test::protos::spacecorp::JumpDriveStatus;

fn main() -> Result<(), Box<dyn Error>> {
    let status = Arc::new(Mutex::new(JumpDriveStatus::default()));

    let main_window = MainWindow::new().unwrap();
    main_window.set_powered_time_secs(status.lock().unwrap().powered_time_secs);
    main_window.run().unwrap();

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

    // do slint stuff here

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
