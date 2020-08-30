// cargo run --example iteration_counter -- -w 3
extern crate timely;

use timely::dataflow::operators::*;
use timely::dataflow::Scope;

fn main() {
    timely::example(|scope| {
        let stream = (0..10).to_stream(scope);
        stream.inspect_batch(move |t, xs| {
            println!(
                "\n===============\nOutside the nested loop\nt of Stream is a &u64 {:?}\nxs array of i32 is {:?}",
                t, xs
            )
        });
        // println!("stream is {:?}", stream);
        // Create a new scope with a (u64, u32) timestamp.
        let result = scope.iterative::<u32, _, _>(|subscope| {
            stream
                .enter(subscope)
                .inspect_batch(|t, xs| {
                    println!(
                        "\n===============\nInside the nested loop\nt is a prodcut of <u64, u32> {:?}\nxs array of i32 is {:?}",
                        t, xs
                    )
                })
                .leave()
        });
        result.inspect_batch(move |t, xs| {
            println!(
                "\n===============\nOutside the nested loop\nt is a &u64 {:?}\nxs array of i32 is {:?}",
                t, xs
            )
        });
    });
}
