extern crate colored;
extern crate timely;

use colored::*;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use timely::dataflow::operators::{Exchange, Input, Inspect, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};
const THREAD_COLORS: &'static [&'static str] = &["magenta", "green", "red", "blue", "yellow"];
fn main() {
    // construct and execute a timely dataflow
    timely::execute_from_args(std::env::args(), |worker| {
        // add an input and base computation off of it
        let index = worker.index();
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();
        let closure1 = move |x: &u64| {
            println!("worker {}:\thello {:?}", index, x);
        };

        let closure2 = move |t: &u64, xs: &[u64]| {
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
        };
        // create a new input, exchange data, and inspect its output
        worker.dataflow(|scope| {
            scope
                .input_from(&mut input)
                .exchange(|x| *x)
                .inspect(closure1)
                .inspect_batch(closure2)
                .probe_with(&mut probe);
        });
        let delay_sec = Duration::from_secs(3);
        let index = worker.index();
        // introduce input, advance computation
        for round in 0..4 {
            let thread_color = THREAD_COLORS[index].to_string();
            println!(
                "{} {:?} \t {} {:?} \t {} {:?}",
                "\n=========\nCurrent Input Time is "
                    .color(thread_color.clone())
                    .bold(),
                input.time(),
                "In worker ".color(thread_color.clone()).bold(),
                index,
                "The real time is ".color(thread_color.clone()).bold(),
                t_now()
            );
            input.send(round);
            sleep(delay_sec);
            input.advance_to(round + 1);
            sleep(delay_sec);
            while probe.less_than(input.time()) {
                worker.step();
                println!(
                    "{} {:?} {}",
                    " worker ".color(thread_color.clone()).bold(),
                    index,
                    "is doing one more step".color(thread_color.clone()).bold()
                );
            }
            sleep(delay_sec);
        }
    })
    .unwrap();
}

fn t_now() -> u64 {
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH);
    return now.unwrap().as_secs();
}
//
