// To run with 3 processors
//  cargo run --example wordcount -- -n 3 -p 0
// cargo run --example wordcount -- -n 3 -p 1
// cargo run --example wordcount -- -n 3 -p 2

use colored::*;
extern crate timely;

use std::collections::HashMap;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Inspect, Map, Operator, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};

const THREAD_COLORS: &'static [&'static str] = &["magenta", "green", "red", "blue", "yellow"];

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        println!("ThreadColors is {:?}", THREAD_COLORS);
        println!("ThreadColors size is :{}", THREAD_COLORS.len());
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();
        let index = worker.index();

        // println!("{} {:?}", "worker".color(thread_color).bold(), index.to_string());
        // define a distribution function for strings.
        let exchange = Exchange::new(|x: &(String, i64, usize, u64)| (x.0).len() as u64);

        // create a new input, exchange data, and inspect its output
        worker.dataflow::<usize, _, _>(|scope| {
            input
                .to_stream(scope)
                .flat_map(
                    |(text, diff, worker_index, __worker_peers, t): (
                        String,
                        i64,
                        usize,
                        usize,
                        u64,
                    )| {
                        text.split_whitespace()
                            .map(move |word| (word.to_owned(), diff, worker_index, t))
                            .collect::<Vec<_>>()
                    },
                )
                .unary_frontier(exchange, "WordCount", |_capability, _info| {
                    let mut queues = HashMap::new();
                    let mut counts = HashMap::new();

                    move |input, output| {
                        while let Some((time, data)) = input.next() {
                            queues
                                .entry(time.retain())
                                .or_insert(Vec::new())
                                .push(data.replace(Vec::new()));
                        }

                        for (key, val) in queues.iter_mut() {
                            if !input.frontier().less_equal(key.time()) {
                                let mut session = output.session(key);
                                for mut batch in val.drain(..) {
                                    for (word, diff, worker_index, t) in batch.drain(..) {
                                        let entry = counts.entry(word.clone()).or_insert(0i64);
                                        *entry += diff;
                                        session.give((word, *entry, worker_index, t));
                                    }
                                }
                            }
                        }

                        queues.retain(|_key, val| !val.is_empty());
                    }
                })
                .inspect(move |x| {
                    let thread_color = THREAD_COLORS[index].to_string();
                    // println!("{} {:?}", "worker".color(thread_color.clone()).bold(), index.to_string());
                    println!(
                        "{} {} {} {:?} {} {:?}",
                        "worker ".color(thread_color.clone()).bold(),
                        index,
                        ": seen: ".color(thread_color.clone()).bold(),
                        x,
                        " @ real time : ".color(thread_color.clone()).bold(),
                        t_now()
                    );
                })
                .probe_with(&mut probe);
        });

        // introduce data and watch!
        for round in 0..4 {
            let peers = worker.peers();
            let waiting_sec = Duration::from_secs(2);
            let index = worker.index();
            let thread_color = THREAD_COLORS[index].to_string();
            println!(
                "{} {:?} \t {} {} \t {} {:?}",
                "\n=========\nCurrent Input Time is "
                    .color(thread_color.clone())
                    .bold(),
                input.time(),
                "In worker ".color(thread_color.clone()).bold(),
                index,
                "The real time is ".color(thread_color.clone()).bold(),
                t_now()
            );

            input.send((
                "the words the architect is here".to_owned(),
                1,
                index,
                peers,
                t_now(),
            ));
            sleep(waiting_sec);
            input.advance_to(round + 1);
            sleep(waiting_sec);
            while probe.less_than(input.time()) {
                println!(
                    "{} {:?} \t {} \t @ {} {}",
                    "In worker ".color(thread_color.clone()).bold(),
                    index,
                    " Performing one more work step".color(thread_color.clone()).bold(), 
                    "The real time is ".color(thread_color.clone()).bold(),
                    t_now()
                );
                worker.step();
            }
            sleep(waiting_sec);
        }
    })
    .unwrap();
}

fn t_now() -> u64 {
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH);
    return now.unwrap().as_secs();
}
