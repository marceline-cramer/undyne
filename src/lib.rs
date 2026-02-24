use std::{collections::BTreeMap, fmt::Debug, marker::PhantomData};

use parking_lot::Mutex;

#[cfg(test)]
mod tests;

pub trait KeyValueNodeExt<T: Time>: KeyValueNode<T, Key: Ord, Value: Ord> {
    fn join<R: Ord + Data>(
        self,
        rhs: impl KeyValueNode<T, Input = Self::Input, Key = Self::Key, Value = R>,
    ) -> impl NodeExt<T, Input = Self::Input, Output = (Self::Key, Self::Value, R)> {
        Join(
            NaiveArrangement::<T, _>::new(self),
            NaiveArrangement::<T, _>::new(rhs),
        )
    }
}

impl<T: PartialOrd + Data, N: KeyValueNode<T, Key: Ord, Value: Ord>> KeyValueNodeExt<T> for N {}

pub struct Join<L, R>(L, R);

impl<T, K, I, L, R> Node<T> for Join<L, R>
where
    I: Data,
    K: Data,
    L: Send + Sync,
    R: Send + Sync,
    T: Time,
    L: Arranged<T, Key = K, Input = I>,
    R: Arranged<T, Key = K, Input = I>,
{
    type Input = I;
    type Output = (K, L::Value, R::Value);

    fn update(
        &self,
        input: &(Self::Input, T, isize),
        output: impl Fn(&(Self::Output, T, isize)) + Send + Sync,
    ) {
        self.0.update(input, |((key, lhs), time, ldiff)| {
            self.1.aggregate(time, key, |sums| {
                for (rhs, rdiff) in sums {
                    let value = (key.clone(), lhs.clone(), rhs.clone());
                    output(&(value, time.clone(), ldiff * rdiff));
                }
            });
        });

        self.1.update(input, |((key, rhs), time, rdiff)| {
            self.0.aggregate(time, key, |sums| {
                for (lhs, ldiff) in sums {
                    let value = (key.clone(), lhs.clone(), rhs.clone());
                    output(&(value, time.clone(), ldiff * rdiff));
                }
            });
        });
    }
}

pub struct NaiveArrangement<T: Time, N: KeyValueNode<T>> {
    node: N,
    history: Mutex<Vec<(T, N::Key, N::Value, isize)>>,
}

impl<T: Time, N: KeyValueNode<T>> NaiveArrangement<T, N> {
    pub fn new(node: N) -> Self {
        Self {
            node,
            history: Mutex::new(Vec::new()),
        }
    }
}

impl<N, T> Arranged<T> for NaiveArrangement<T, N>
where
    N: KeyValueNode<T, Key: Eq, Value: Ord>,
    T: PartialOrd + Data,
{
    fn aggregate(&self, time: &T, key: &Self::Key, output: impl Fn(&[(Self::Value, isize)])) {
        let mut sums = BTreeMap::new();
        for (prev_time, prev_key, value, diff) in self.history.lock().iter() {
            if prev_key != key {
                continue;
            }

            if !prev_time.less_equal(time) {
                continue;
            }

            *sums.entry(value.clone()).or_default() += diff;
        }

        eprintln!("{key:?}@{time:?}: {sums:?}");

        if sums.is_empty() {
            return;
        }

        let as_vec: Vec<_> = sums.into_iter().collect();
        output(as_vec.as_slice());
    }
}

impl<N, T> Node<T> for NaiveArrangement<T, N>
where
    N: KeyValueNode<T>,
    T: Time,
{
    type Input = N::Input;
    type Output = N::Output;

    fn update(
        &self,
        input: &(Self::Input, T, isize),
        output: impl Fn(&(Self::Output, T, isize)) + Send + Sync,
    ) {
        self.node.update(input, |update| {
            let ((key, value), time, diff) = update;
            let history = (time.clone(), key.clone(), value.clone(), *diff);
            output(update);
            self.history.lock().push(history);
        });
    }
}

pub trait Arranged<T: Time>: KeyValueNode<T, Key: Eq> {
    fn aggregate(&self, time: &T, key: &Self::Key, output: impl Fn(&[(Self::Value, isize)]));
}

pub trait KeyValueNode<T: Time>: Node<T, Output = (Self::Key, Self::Value)> {
    type Key: Data;
    type Value: Data;
}

impl<T, K, V, N> KeyValueNode<T> for N
where
    T: Time,
    K: Data,
    V: Data,
    N: Node<T, Output = (K, V)>,
{
    type Key = K;
    type Value = V;
}

pub trait NodeExt<T: Time>: Node<T> {
    fn filter(
        self,
        op: impl Fn(&Self::Output) -> bool + Send + Sync,
    ) -> impl NodeExt<T, Input = Self::Input, Output = Self::Output> {
        Chain(self, Filter(op, PhantomData))
    }

    fn map<D: Data>(
        self,
        op: impl Fn(&Self::Output) -> D + Send + Sync,
    ) -> impl NodeExt<T, Input = Self::Input, Output = D> {
        Chain(self, Map(op, PhantomData, PhantomData))
    }

    fn flat_map<D: IntoIterator<Item: Data> + Send + Sync>(
        self,
        op: impl Fn(&Self::Output) -> D + Send + Sync,
    ) -> impl NodeExt<T, Input = Self::Input, Output = D::Item> {
        Chain(self, FlatMap(op, PhantomData, PhantomData))
    }

    fn concat(
        self,
        other: impl NodeExt<T, Input = Self::Input, Output = Self::Output>,
    ) -> impl NodeExt<T, Input = Self::Input, Output = Self::Output>
    where
        Self::Input: Clone,
    {
        Concat(self, other)
    }

    fn fixedpoint<N>(
        self,
        scope: impl FnOnce(Input<Self::Output, (T, usize)>) -> N,
    ) -> impl NodeExt<T, Input = Self::Input, Output = Self::Output>
    where
        N: Node<(T, usize), Input = Self::Output, Output = Self::Output>,
    {
        Chain(
            self,
            Fixedpoint(scope(Input {
                _data: PhantomData,
                _time: PhantomData,
            })),
        )
    }
}

impl<T: Time, N: Node<T>> NodeExt<T> for N {}

pub struct Input<D, T = usize> {
    _data: PhantomData<D>,
    _time: PhantomData<T>,
}

impl<D, T> Copy for Input<D, T> {}

#[allow(clippy::non_canonical_clone_impl)]
impl<D, T> Clone for Input<D, T> {
    fn clone(&self) -> Self {
        Self {
            _data: PhantomData,
            _time: PhantomData,
        }
    }
}

impl<T: Time, D: Data> Node<T> for Input<D, T> {
    type Input = D;
    type Output = D;

    fn update(
        &self,
        input: &(Self::Input, T, isize),
        output: impl Fn(&(Self::Output, T, isize)) + Send + Sync,
    ) {
        output(input);
    }
}

impl<D> Default for Input<D, usize> {
    fn default() -> Self {
        Self {
            _data: PhantomData,
            _time: PhantomData,
        }
    }
}

impl<D> Input<D, usize> {
    pub fn new() -> Self {
        Self::default()
    }
}

pub struct Map<F, I, O>(F, PhantomData<I>, PhantomData<O>);

impl<T, F, I, O> Node<T> for Map<F, I, O>
where
    T: Time,
    F: Fn(&I) -> O + Send + Sync,
    I: Data,
    O: Data,
{
    type Input = I;
    type Output = O;

    fn update(
        &self,
        input: &(Self::Input, T, isize),
        output: impl Fn(&(Self::Output, T, isize)) + Send + Sync,
    ) {
        let (data, time, diff) = input;
        let data = self.0(data);
        output(&(data, time.clone(), *diff));
    }
}

pub struct Filter<F, D>(F, PhantomData<D>);

impl<F, D, T> Node<T> for Filter<F, D>
where
    T: Time,
    F: Fn(&D) -> bool + Send + Sync,
    D: Data,
{
    type Input = D;
    type Output = D;

    fn update(
        &self,
        input: &(Self::Input, T, isize),
        output: impl Fn(&(Self::Output, T, isize)) + Send + Sync,
    ) {
        let (data, time, diff) = input;
        if self.0(data) {
            output(&(data.clone(), time.clone(), *diff));
        }
    }
}

pub struct FlatMap<F, I, O>(F, PhantomData<I>, PhantomData<O>);

impl<T, F, I, O> Node<T> for FlatMap<F, I, O>
where
    T: Time,
    F: Fn(&I) -> O + Send + Sync,
    I: Data,
    O: IntoIterator<Item: Data> + Send + Sync,
{
    type Input = I;
    type Output = O::Item;

    fn update(
        &self,
        input: &(Self::Input, T, isize),
        output: impl Fn(&(Self::Output, T, isize)) + Send + Sync,
    ) {
        let (data, time, diff) = input;
        self.0(data).into_iter().for_each(|data| {
            output(&(data, time.clone(), *diff));
        });
    }
}

pub struct Fixedpoint<N>(N);

impl<D, T, N> Node<T> for Fixedpoint<N>
where
    D: Data,
    T: Time,
    N: Node<(T, usize), Input = D, Output = D>,
{
    type Input = D;
    type Output = D;

    fn update(
        &self,
        input: &(Self::Input, T, isize),
        output: impl Fn(&(Self::Output, T, isize)) + Send + Sync,
    ) {
        let (data, time, diff) = input;
        let mut stack = Vec::with_capacity(1024);
        stack.push((data.clone(), (time.clone(), 0usize), *diff));

        while let Some(update) = stack.pop() {
            let (data, (time, _stratum), diff) = &update;
            output(&(data.clone(), time.clone(), *diff));

            let stack = Mutex::new(&mut stack);
            self.0.update(&update, |(data, (time, stratum), diff)| {
                let update = (data.clone(), (time.clone(), *stratum + 1), *diff);
                stack.lock().push(update);
            });
        }
    }
}

pub struct Concat<L, R>(L, R);

impl<I, O, T, L, R> Node<T> for Concat<L, R>
where
    I: Data,
    O: Data,
    T: Time,
    L: Node<T, Input = I, Output = O>,
    R: Node<T, Input = I, Output = O>,
{
    type Input = I;
    type Output = O;

    fn update(
        &self,
        input: &(Self::Input, T, isize),
        output: impl Fn(&(Self::Output, T, isize)) + Send + Sync,
    ) {
        self.0.update(input, &output);
        self.1.update(input, &output);
    }
}

pub struct Chain<L, R>(pub L, pub R);

impl<D, T, L, R> Node<T> for Chain<L, R>
where
    D: Data,
    T: Time,
    L: Node<T, Output = D>,
    R: Node<T, Input = D>,
{
    type Input = L::Input;
    type Output = R::Output;

    fn update(
        &self,
        input: &(Self::Input, T, isize),
        output: impl Fn(&(Self::Output, T, isize)) + Send + Sync,
    ) {
        self.0
            .update(input, |update| self.1.update(update, &output))
    }
}

pub trait Node<T: Time>: Send + Sync + Sized {
    type Input: Data;
    type Output: Data;

    fn update(
        &self,
        input: &(Self::Input, T, isize),
        output: impl Fn(&(Self::Output, T, isize)) + Send + Sync,
    );
}

impl<T: Time, N: Node<T>> Node<T> for &N {
    type Input = N::Input;
    type Output = N::Output;

    fn update(
        &self,
        input: &(Self::Input, T, isize),
        output: impl Fn(&(Self::Output, T, isize)) + Send + Sync,
    ) {
        (*self).update(input, output)
    }
}

pub trait Time: Data + PartialOrd {}

impl<T: Data + PartialOrd> Time for T {}

pub trait Data: Clone + Debug + Send + Sync {}

impl<T: Clone + Debug + Send + Sync> Data for T {}

pub trait PartialOrd<Rhs: ?Sized = Self>: PartialEq<Rhs> {
    fn less_than(&self, other: &Rhs) -> bool;

    fn less_equal(&self, other: &Rhs) -> bool;
}

pub trait AutoPartialOrd: Ord {}

impl<T: AutoPartialOrd> PartialOrd<T> for T {
    fn less_than(&self, other: &T) -> bool {
        self < other
    }

    fn less_equal(&self, other: &T) -> bool {
        self <= other
    }
}

macro_rules! impl_auto_partial_ord {
    () => {};

    ($head:ty, $($tail:ty,)*) => {
        impl AutoPartialOrd for $head {}
        impl_auto_partial_ord!($($tail,)*);
    };
}

impl_auto_partial_ord!(
    usize, u8, u16, u32, u64, u128, isize, i8, i16, i32, i64, i128,
);

impl<L: PartialOrd, R: PartialOrd> PartialOrd for (L, R) {
    fn less_than(&self, other: &Self) -> bool {
        self.0.less_than(&other.0) && self.1.less_than(&other.1)
    }

    fn less_equal(&self, other: &Self) -> bool {
        self.0.less_equal(&other.0) && self.1.less_equal(&other.1)
    }
}
