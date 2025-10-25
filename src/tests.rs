use std::fmt::Debug;

use super::*;

#[test]
fn test_map_squares() {
    let max: i32 = 10;
    let origin = Origin::from_initial(0..max);
    let expected = (0..max).map(|i| i * i);
    let mut mapped = origin.map(|i| i * i);
    expect_unit_weights(&mut mapped, expected);
}

#[test]
fn test_equijoin_instant() {
    let left = [(0, 0), (1, 1), (2, 2)];
    let right = [(0, 1), (1, 2), (2, 3)];
    let expected = [(0, 0, 1), (1, 1, 2), (2, 2, 3)];
    let mut join = Origin::from_initial(left).join(Origin::from_initial(right));
    expect_unit_weights(&mut join, expected);
}

#[test]
fn test_equijoin_defer_left() {
    let left = Origin::from_batches([vec![], vec![(0, 0), (1, 1), (2, 2)]]);
    let right = Origin::from_batches([vec![(0, 1), (1, 2), (2, 3)], vec![]]);

    // first batch should have no joined values
    let mut join = left.join(right);
    expect_unit_weights(&mut join, None);

    // second batch should introduce all joined values
    let expected = [(0, 0, 1), (1, 1, 2), (2, 2, 3)];
    expect_unit_weights(&mut join, expected);
}

#[test]
fn test_equijoin_defer_right() {
    let left = Origin::from_batches([vec![(0, 0), (1, 1), (2, 2)], vec![]]);
    let right = Origin::from_batches([vec![], vec![(0, 1), (1, 2), (2, 3)]]);

    // first batch should have no joined values
    let mut join = left.join(right);
    expect_unit_weights(&mut join, None);

    // second batch should introduce all joined values
    let expected = [(0, 0, 1), (1, 1, 2), (2, 2, 3)];
    expect_unit_weights(&mut join, expected);
}

fn expect_unit_weights<N: Node>(node: &mut N, expected: impl IntoIterator<Item = N::Item>)
where
    N::Item: Clone + Debug + Ord + Send + Sync,
{
    let expected_weights: OrdMap<_, _> = expected.into_iter().map(|val| (val, 1i16)).collect();
    assert_eq!(node.consolidate(), expected_weights);
}
