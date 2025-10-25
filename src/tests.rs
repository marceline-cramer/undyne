use std::collections::HashSet;

use super::*;

#[test]
fn test_map_squares() {
    let max: i32 = 10;
    let origin = Origin::from_initial(0..max);
    let expected: HashSet<_> = (0..max).map(|i| i * i).map(Update::insert).collect();
    let mut mapped = origin.map(|i| i * i);
    let got: HashSet<_> = mapped.update().collect();
    assert_eq!(expected, got);
}

#[test]
fn test_equijoin_sequence() {
    let left = [(0, 0), (1, 1), (2, 2)];
    let right = [(0, 1), (1, 2), (2, 3)];
    let expected = [(0, 0, 1), (1, 1, 2), (2, 2, 3)];
    let expected: HashSet<_> = expected.map(Update::insert).into_iter().collect();
    let mut join = Origin::from_initial(left).join(Origin::from_initial(right));
    let got: HashSet<_> = join.update().collect();
    assert_eq!(HashSet::from_iter(expected), got);
}
