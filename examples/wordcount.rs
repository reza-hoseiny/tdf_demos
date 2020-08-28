// To run with 3 processors
// cargo run --example wordcount -- -n 3 -p 0
// cargo run --example wordcount -- -n 3 -p 1
// cargo run --example wordcount -- -n 3 -p 2

extern crate timely;

use std::collections::HashMap;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Inspect, Map, Operator, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();
        let index = worker.index();
        // define a distribution function for strings.
        let exchange = Exchange::new(|x: &(String, i64, usize, u64)| (x.0).len() as u64);

        // create a new input, exchange data, and inspect its output
        worker.dataflow::<usize, _, _>(|scope| {
            input
                .to_stream(scope)
                .flat_map(|(text, diff, worker_index, __worker_peers, t ): (String, i64, usize, usize, u64)| {
                    text.split_whitespace()
                        .map(move |word| (word.to_owned(), diff, worker_index, t))
                        .collect::<Vec<_>>()
                })
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
                    
                    println!("worker {}: seen: {:?} @ {:?}", index, x, t_now());                    
                }
            )
                .probe_with(&mut probe);
        });

        // introduce data and watch!
        for round in 0..4 {
            let peers = worker.peers();
            let three_sec = Duration::from_secs(1);
            let index = worker.index();

            println!(
                "\n=========\nCurrent Input Time is {:?} \t In worker {} \t The real time is {:?}",
                input.time(),
                index,
                t_now()
            );

            input.send(("the word the arche".to_owned(), 1, index, peers, t_now()));
            sleep(three_sec);
            input.advance_to(round + 1);
            sleep(three_sec);
            while probe.less_than(input.time()) {
                worker.step();
            }
            sleep(three_sec);
        }
    })
    .unwrap();
}

fn t_now() -> u64 {
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH);
    return now.unwrap().as_secs();
}
