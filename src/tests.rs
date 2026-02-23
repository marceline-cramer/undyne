use std::{collections::BTreeMap, fmt::Debug, sync::Mutex};

use crate::*;

fn expect_result<N: Node<usize, Input: Clone, Output: Debug + Ord>>(
    node: N,
    inputs: impl IntoIterator<Item = N::Input>,
    outputs: impl IntoIterator<Item = N::Output>,
) {
    // will switch this to single batch once batching is supported
    let got = Mutex::new(BTreeMap::new());
    for input in inputs.into_iter().map(|data| (data.clone(), 0, 1)) {
        node.update(&input, |(output, time, diff)| {
            let mut got = got.lock().unwrap();
            let weight = got.entry(output.clone()).or_insert(0);
            *weight += *diff;
            assert_eq!(*time, 0);
        });
    }

    let got = got.into_inner().unwrap();
    let expected: BTreeMap<_, _> = outputs.into_iter().map(|output| (output, 1)).collect();
    assert_eq!(got, expected);
}

#[test]
fn test_map() {
    let node = Input::<usize>::new().map(|i| *i * *i);
    expect_result(node, [0, 1, 2, 3, 4], [0, 1, 4, 9, 16]);
}

#[test]
fn test_filter() {
    let node = Input::<usize>::new().filter(|i| *i % 2 == 0);
    expect_result(node, [0, 1, 2, 3, 4, 5, 6], [0, 2, 4, 6]);
}

#[test]
fn test_flat_map() {
    let node = Input::<usize>::new().flat_map(|i| [*i, *i * *i]);
    expect_result(node, [3, 5, 7, 11], [3, 5, 7, 9, 11, 25, 49, 121]);
}

#[test]
fn test_fixedpoint_range() {
    let node = Input::<usize>::new()
        .fixedpoint(|scope| scope.map(|count| *count + 1).filter(|count| *count < 10));

    expect_result(node, [0], 0..10);
}

#[test]
fn test_join() {
    let input = Input::<(usize, usize)>::new();
    let node = input.join(input);
    expect_result(node, [(0, 1), (1, 2)], [(0, 1, 2)]);
}
