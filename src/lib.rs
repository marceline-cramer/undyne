use std::marker::PhantomData;

use parking_lot::Mutex;

#[cfg(test)]
mod tests;

pub trait KeyValueNodeExt<T: Copy + Data>: KeyValueNode<T> {
    fn join<R: Data>(
        self,
        rhs: impl KeyValueNode<T, Input = Self::Input, Key = Self::Key, Value = R>,
    ) -> impl NodeExt<T, Input = Self::Input, Output = (Self::Key, Self::Value, R)> {
        Join(self, rhs)
    }
}

impl<T: Copy + Data, N: KeyValueNode<T>> KeyValueNodeExt<T> for N {}

pub struct Join<L, R>(L, R);

impl<T, K, I, L, R> Node<T> for Join<L, R>
where
    I: Data,
    K: Data,
    L: KeyValueNode<T, Key = K, Input = I>,
    R: KeyValueNode<T, Key = K, Input = I>,
{
    type Input = I;
    type Output = (K, L::Value, R::Value);

    fn update(
        &self,
        input: &(Self::Input, T, isize),
        output: impl Fn(&(Self::Output, T, isize)) + Send + Sync,
    ) {
        todo!();
    }
}

pub trait KeyValueNode<T>: Node<T, Output = (Self::Key, Self::Value)> {
    type Key: Data;
    type Value: Data;
}

impl<T, K, V, N> KeyValueNode<T> for N
where
    K: Data,
    V: Data,
    N: Node<T, Output = (K, V)>,
{
    type Key = K;
    type Value = V;
}

pub trait NodeExt<T: Copy + Send + Sync>: Node<T> {
    fn filter(
        self,
        op: impl Fn(&Self::Output) -> bool + Send + Sync,
    ) -> impl NodeExt<T, Input = Self::Input, Output = Self::Output> {
        Chain(self, Filter(op, PhantomData))
    }

    fn map<D: Clone + Send + Sync>(
        self,
        op: impl Fn(&Self::Output) -> D + Send + Sync,
    ) -> impl NodeExt<T, Input = Self::Input, Output = D> {
        Chain(self, Map(op, PhantomData, PhantomData))
    }

    fn flat_map<D: IntoIterator<Item: Clone + Send + Sync> + Send + Sync>(
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

impl<T: Copy + Data, N: Node<T>> NodeExt<T> for N {}

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

impl<T: Send + Sync, D: Clone + Send + Sync> Node<T> for Input<D, T> {
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
    T: Copy,
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
        output(&(data, *time, *diff));
    }
}

pub struct Filter<F, D>(F, PhantomData<D>);

impl<F, D, T> Node<T> for Filter<F, D>
where
    T: Copy,
    F: Fn(&D) -> bool + Send + Sync,
    D: Clone + Send + Sync,
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
            output(&(data.clone(), *time, *diff));
        }
    }
}

pub struct FlatMap<F, I, O>(F, PhantomData<I>, PhantomData<O>);

impl<T, F, I, O> Node<T> for FlatMap<F, I, O>
where
    T: Copy,
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
            output(&(data, *time, *diff));
        });
    }
}

pub struct Fixedpoint<N>(N);

impl<D, T, N> Node<T> for Fixedpoint<N>
where
    D: Data,
    T: Copy + Send + Sync,
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
        stack.push((data.clone(), (*time, 0usize), *diff));

        while let Some(update) = stack.pop() {
            let (data, (time, _stratum), diff) = &update;
            output(&(data.clone(), *time, *diff));

            let stack = Mutex::new(&mut stack);
            self.0.update(&update, |(data, (time, stratum), diff)| {
                let update = (data.clone(), (*time, *stratum + 1), *diff);
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

pub trait Node<T>: Send + Sync + Sized {
    type Input: Data;
    type Output: Data;

    fn update(
        &self,
        input: &(Self::Input, T, isize),
        output: impl Fn(&(Self::Output, T, isize)) + Send + Sync,
    );
}

impl<T, N: Node<T>> Node<T> for &N {
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

pub trait Data: Clone + Send + Sync {}

impl<T: Clone + Send + Sync> Data for T {}
