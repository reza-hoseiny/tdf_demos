extern crate colored;
extern crate timely;

use colored::*;
use std::convert::TryFrom;
use std::thread::sleep;
use std::time::{Duration, SystemTime};

use timely::dataflow::operators::{
    Concat, ConnectLoop, Feedback, Filter,Exchange, Inspect, Map, Partition, ToStream,
}; //BranchWhen, Feedback, Exchange,

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
                "after Exchange: worker ".color(thread_color.clone()).bold(),
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
            let res = u64::try_from(30 * (index.clone()+ 1));
            let ret: u64 = res.unwrap() + x + 1u64;
            sleep(delay_sec);
            ret
        };

        let closure_before_map = move |x: &u64| {
            println!(
                "Before applying Map : worker {}:\thello {:?} real time is {:?} ",
                index,
                x,
                t_now()
            );
        };

        let closure1 = move |x: &u64| {
            println!(
                "Afer Map: worker {}:\thello {:?} real time is {:?} ",
                index,
                x,
                t_now()
            );
        };
        // create a new input, exchange data, and inspect its output
        worker.dataflow::<u64, _, _>(move |scope| {
            // create a loop that cycles unboundedly.
            let (handle0, stream0) = scope.feedback(40);
            let (handle1, stream1) = scope.feedback(40);

            let results0 = stream0.map(map_closure).filter(|x| *x < 300);
            let results1 = stream1.map(map_closure).filter(|x| *x < 600);

            let parts = (0..10)
                .to_stream(scope)
                .concat(&results0)
                .concat(&results1)
                .exchange(|&x| x)
                .inspect(closure1)
                .inspect_batch(closure2).partition(2, |x| (x % 2, x));

            parts[0].connect_loop(handle0);
            parts[1].connect_loop(handle1);
            // (0..10)
            //     .to_stream(scope)
            //     .concat(&cycle)
            //     .inspect(closure_before_map)
            //     .map(map_closure)
            //     .inspect(closure1)
            //     .exchange(|&x| x)
            //     .inspect_batch(closure2)
            //     .branch_when(|t| t < &200)
            //     .1
            //     .connect_loop(handle);
        });
    })
    .unwrap();
}

fn t_now() -> u64 {
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH);
    return now.unwrap().as_secs();
}
