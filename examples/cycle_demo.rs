extern crate timely;

use std::thread::sleep;
use std::time::{Duration, SystemTime};
use timely::dataflow::operators::*;
use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Exchange, Inspect, Probe};
use timely::dataflow::Scope;

fn main() {
    // construct and execute a timely dataflow
    timely::execute_from_args(std::env::args(), |worker| {
        // add an input and base computation off of it
        let index = worker.index();
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // create a new input, exchange data, and inspect its output
        worker.dataflow(|scope| {
            scope.input_from(&mut input)
                 .exchange(|x| *x)
                 .inspect(move |x| println!("worker {}:\thello {}", index, x))
                 .inspect_batch(move |t, xs| {
                    println!("In worker {} t is: {:?}, xs data is: {:?} @ real time is {:?}", index, t, xs, t_now());
                }).probe_with(&mut probe);
        });
        let three_sec = Duration::from_secs(1);
        let index = worker.index();
        // introduce input, advance computation
        for round in 0..4 {
            println!(
                "\n=========\nCurrent Input Time is {:?} \t In worker {} \t The real time is {:?}",
                input.time(),
                index,
                t_now()
            );
            input.send(round);
            sleep(three_sec);
            input.advance_to(round + 1);
            sleep(three_sec);
            worker.step();
            sleep(three_sec);
        }
    });
}

fn t_now() -> u64 {
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH);
    return now.unwrap().as_secs();
}
