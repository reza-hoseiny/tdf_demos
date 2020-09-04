extern crate colored;
extern crate timely;

use colored::*;
use std::convert::TryFrom;
use std::thread::sleep;
use std::time::{Duration, SystemTime};

use timely::dataflow::operators::{
    BranchWhen, Concat, ConnectLoop, Exchange, Feedback, Inspect, Map, ToStream,
};

const THREAD_COLORS: &'static [&'static str] = &["magenta", "green", "red", "blue", "yellow"];
fn main() {
    // construct and execute a timely dataflow
    timely::execute_from_args(std::env::args(), |worker| {
        // add an input and base computation off of it
        let index = worker.index();

        let closure2 = move |t: &u64, xs: &[u64]| {
            let delay_sec = Duration::from_secs(0);
            let thread_color = THREAD_COLORS[index].to_string();
            println!(
                "{} {:?} {} {:?} {} {:?} {} {:?}",
                "worker ".color(thread_color.clone()).bold(),
                index,
                " t is: ".color(thread_color.clone()).bold(),
                t,
                ": seen: ".color(thread_color.clone()).bold(),
                xs,
                " @ real time : ".color(thread_color.clone()).bold(),
                t_now()
            );
            sleep(delay_sec);
        };

        let map_closure = move |x: u64| {
            let delay_sec = Duration::from_secs(1);
            let res = u64::try_from(100 * (index.clone() ));
            let ret: u64 = res.unwrap() + x;
            sleep(delay_sec);
            ret
        };
        // create a new input, exchange data, and inspect its output
        worker.dataflow(|scope| {
            let (handle, cycle) = scope.feedback(40);
            (0..10)
                .to_stream(scope)
                .concat(&cycle)
                .map(map_closure)
                .exchange(|&x| x)
                .inspect_batch(closure2)
                .branch_when(|t| t < &600)
                .1
                .connect_loop(handle);
        });
    })
    .unwrap();
}

fn t_now() -> u64 {
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH);
    return now.unwrap().as_secs();
}
