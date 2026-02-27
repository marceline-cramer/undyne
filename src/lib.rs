use std::{collections::BTreeMap, fmt::Debug, marker::PhantomData, sync::Arc};

use parking_lot::Mutex;

#[cfg(test)]
mod tests;

pub trait PrefixNodeExt<T: Time>: Node<T, Output = (Self::Key, Self::Value)> {
    type Key: Data + Ord;
    type Value: Data;

    fn prefix<N>(
        self,
        factory: impl Fn(Self::Key, Input<Self::Value, T>) -> N + Send + Sync,
    ) -> impl NodeExt<T, Input = Self::Input, Output = N::Output>
    where
        N: Node<T, Input = Self::Value>,
    {
        Chain(
            self,
            Prefix {
                factory: move |key| factory(key, Input::new_internal()),
                keys: Mutex::new(BTreeMap::new()),
            },
        )
    }

    fn join<N>(
        self,
        rhs: N,
    ) -> impl NodeExt<T, Input = Self::Input, Output = (Self::Key, Self::Value, N::Value)>
    where
        Self::Value: Ord,
        N: PrefixNodeExt<T, Input = Self::Input, Key = Self::Key, Value: Ord>,
    {
        let left = self.map(|(key, value)| (key.clone(), Either::Left(value.clone())));
        let right = rhs.map(|(key, value)| (key.clone(), Either::Right(value.clone())));
        let either = Fork(left, right);

        either.prefix(|key, scope| {
            let left = NaiveArrangement::<T, Self::Value>::default();
            let right = NaiveArrangement::<T, N::Value>::default();
            let product = Product::new(left, right);
            Chain(scope, product).map(move |(l, r)| (key.clone(), l.clone(), r.clone()))
        })
    }
}

impl<T, N, K, V> PrefixNodeExt<T> for N
where
    T: Time,
    K: Ord + Data,
    V: Data,
    N: Node<T, Output = (K, V)>,
{
    type Key = K;
    type Value = V;
}

pub struct Prefix<F, K, N> {
    factory: F,
    keys: Mutex<BTreeMap<K, Arc<N>>>,
}

impl<T, F, K, N> Node<T> for Prefix<F, K, N>
where
    T: Time,
    F: Fn(K) -> N + Send + Sync,
    K: Data + Ord,
    N: Node<T>,
{
    type Input = (K, N::Input);
    type Output = N::Output;

    fn update(
        &self,
        input: &(Self::Input, T, isize),
        output: impl Fn(&(Self::Output, T, isize)) + Send + Sync,
    ) {
        let ((key, value), time, diff) = input;

        let node = self
            .keys
            .lock()
            .entry(key.to_owned())
            .or_insert_with(|| Arc::new((self.factory)(key.to_owned())))
            .to_owned();

        let update = (value.clone(), time.clone(), *diff);
        node.update(&update, &output);
    }
}

pub struct Product<L, R>(Arc<Mutex<(L, R)>>);

impl<L, R> Clone for Product<L, R> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<L, R> Product<L, R> {
    pub(crate) fn new(left: L, right: R) -> Self {
        Self(Arc::new(Mutex::new((left, right))))
    }
}

impl<T, L, R> Node<T> for Product<L, R>
where
    T: Time,
    L: Arranged<T>,
    R: Arranged<T>,
{
    type Input = Either<L::Data, R::Data>;
    type Output = (L::Data, R::Data);

    fn update(
        &self,
        input: &(Self::Input, T, isize),
        output: impl Fn(&(Self::Output, T, isize)) + Send + Sync,
    ) {
        // lock both halfs to serialize symmetric entries
        let mut guard = self.0.lock();

        let (data, time, inner_diff) = input;
        match data.clone() {
            Either::Left(data) => {
                guard.0.update(&(data.clone(), time.clone(), *inner_diff));

                guard.1.query(time, |outer_data, outer_diff| {
                    let diff = inner_diff * outer_diff;
                    output(&((data.clone(), outer_data.clone()), time.clone(), diff));
                });
            }
            Either::Right(data) => {
                guard.1.update(&(data.clone(), time.clone(), *inner_diff));

                guard.0.query(time, |outer_data, outer_diff| {
                    let diff = inner_diff * outer_diff;
                    output(&((outer_data.clone(), data.clone()), time.clone(), diff));
                });
            }
        }
    }
}

pub struct NaiveArrangement<T, D> {
    history: Vec<(T, D, isize)>,
}

impl<T, D> Default for NaiveArrangement<T, D> {
    fn default() -> Self {
        Self {
            history: Vec::new(),
        }
    }
}

impl<T, D> Arranged<T> for NaiveArrangement<T, D>
where
    T: Time,
    D: Ord + Data,
{
    type Data = D;

    fn update(&mut self, input: &(D, T, isize)) {
        let (data, time, diff) = input;
        let update = (time.clone(), data.clone(), *diff);
        self.history.push(update);
    }

    fn query(&self, time: &T, mut output: impl FnMut(&Self::Data, isize)) {
        let mut sums = BTreeMap::new();
        for (prev_time, data, diff) in self.history.iter() {
            if !prev_time.less_equal(time) {
                continue;
            }

            *sums.entry(data).or_default() += diff;
        }

        eprintln!("{time:?}: {sums:?}");

        for (data, diff) in sums {
            output(data, diff);
        }
    }
}

pub trait Arranged<T: Time>: Send + Sync {
    type Data: Ord + Data;

    fn update(&mut self, input: &(Self::Data, T, isize));

    fn query(&self, time: &T, output: impl Fn(&Self::Data, isize));
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

    fn fork<O, L, R>(
        self,
        scope: impl FnOnce(Input<Self::Output>, Input<Self::Output>) -> (L, R),
    ) -> impl NodeExt<T, Input = Self::Input>
    where
        O: Data,
        L: Node<T, Input = Self::Output, Output = O>,
        R: Node<T, Input = Self::Output, Output = O>,
    {
        let (left, right) = scope(Input::new_internal(), Input::new_internal());
        Chain(self, Fork(left, right))
    }

    fn either<N>(
        self,
        rhs: N,
    ) -> impl NodeExt<T, Input = Self::Input, Output = Either<Self::Output, N::Output>>
    where
        N: Node<T, Input = Self::Input>,
    {
        let left = self.map(|data| Either::Left(data.clone()));
        let right = rhs.map(|data| Either::Right(data.clone()));
        Fork(left, right)
    }

    fn product<N>(
        self,
        rhs: N,
    ) -> impl NodeExt<T, Input = Self::Input, Output = (Self::Output, N::Output)>
    where
        Self::Output: Ord,
        N: Node<T, Input = Self::Input, Output: Ord>,
    {
        let left = NaiveArrangement::<T, Self::Output>::default();
        let right = NaiveArrangement::<T, N::Output>::default();
        let product = Product::new(left, right);
        Chain(self.either(rhs), product)
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
        Self::new_internal()
    }
}

impl<D> Input<D, usize> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<D, T> Input<D, T> {
    pub(crate) fn new_internal() -> Self {
        Self {
            _data: PhantomData,
            _time: PhantomData,
        }
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

pub struct Fork<L, R>(L, R);

impl<I, O, T, L, R> Node<T> for Fork<L, R>
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

#[derive(Clone, Debug)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
}
