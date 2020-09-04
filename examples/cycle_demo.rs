extern crate colored;
extern crate timely;

use colored::*;
use std::convert::TryFrom;
use std::thread::sleep;
use std::time::{Duration, SystemTime};

// use timely::dataflow::operators::{Exchange, Input, Inspect, Probe, Feedback};
// use timely::dataflow::operators::feedback::Feedback;
use timely::dataflow::operators::{
    BranchWhen, Concat, ConnectLoop, Exchange, Feedback, Inspect, Map, ToStream,
};
use timely::dataflow::Scope;
use timely::dataflow::{InputHandle, ProbeHandle};
// use timely::dataflow::operators::*;

const THREAD_COLORS: &'static [&'static str] = &["magenta", "green", "red", "blue", "yellow"];
fn main() {
    // construct and execute a timely dataflow
    timely::execute_from_args(std::env::args(), |worker| {
        // add an input and base computation off of it
        let index = worker.index();
        // let mut input = InputHandle::new();
        // let mut probe = ProbeHandle::new();

        // create a loop that cycles unboundedly.

        let closure1 = move |x: &u64| {
            println!("worker {}:\thello {:?}", index, x);
        };

        // let cyclye_closure =  |cycle| {
        //     cycle.filter(|&x| x >0)
        //     .map(|x| x-1)
        //     .exchange(|&x| x)
        // };

        let closure2 = move |t: &u64, xs: &[u64]| {
            let delay_sec = Duration::from_secs(1);
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
        // create a new input, exchange data, and inspect its output
        worker.dataflow(|scope| {
            // create a loop that cycles unboundedly.
            // let (handle, stream) = scope.feedback(1);
            // let (handle, cylcic_stream) = scope.feedback(1);
            let map_closure = move |x: u64| {                
                let res = u64::try_from(10 * (index.clone()+1));
                let ret: u64 = res.unwrap() + x;                
                ret
            };
            let (handle, cycle) = scope.feedback(1);
            
            (0..10)
                .to_stream(scope)
                .concat(&cycle)
                .map(map_closure)
                .exchange(|&x| x)
                .inspect_batch(closure2)
                .branch_when(|t| t < &14)
                .1
                .connect_loop(handle);

            // scope
            //     .input_from(&mut input)
            //     // .concat(&cylcic_stream)
            //     .inspect(closure1)
            //     .inspect_batch(closure2)
            //     .probe_with(&mut probe);
        });
        // let delay_sec = Duration::from_secs(1);
        // let index = worker.index();
        // introduce input, advance computation
        // for round in 0..4 {
        //     let thread_color = THREAD_COLORS[index].to_string();
        //     println!(
        //         "{} {:?} \t {} {:?} \t {} {:?}",
        //         "\n=========\nCurrent Input Time is "
        //             .color(thread_color.clone())
        //             .bold(),
        //         input.time(),
        //         "In worker ".color(thread_color.clone()).bold(),
        //         index,
        //         "The real time is ".color(thread_color.clone()).bold(),
        //         t_now()
        //     );
        //     input.send(round);
        //     sleep(delay_sec);
        //     input.advance_to(round + 1);
        //     sleep(delay_sec);
        //     while probe.less_than(input.time()) {
        //         worker.step();
        //         println!(
        //             "{} {:?} {}",
        //             " worker ".color(thread_color.clone()).bold(),
        //             index,
        //             "is doing one more step".color(thread_color.clone()).bold()
        //         );
        //     }
        //     sleep(delay_sec);
        // }
    })
    .unwrap();
}

fn t_now() -> u64 {
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH);
    return now.unwrap().as_secs();
}
//
//
// let mut input = computation.scoped(move |scope| {
//     let (input, stream) = scope.new_input();
//     let (helper, cycle) = scope.loop_variable(iterations, 1);
//     stream.concat(&cycle)
//           .exchange(|&x| x)
//           .map(|x| x + 1)
//           .connect_loop(helper);
//     input
// });
