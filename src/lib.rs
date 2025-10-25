use flume::Receiver;
use im::OrdMap;
use parking_lot::Mutex;
use rayon::{prelude::*, yield_now};

#[cfg(test)]
pub mod tests;

/// Efficiently produces the union of two values (typically dataflows)
/// while producing side effects on state `S`.
///
/// This makes it possible to run multiple dataflows of the same type
/// to fixedpoint on disjoint inputs then efficiently unify them.
pub trait Union<S> {
    /// Unions this value with another.
    ///
    /// Returns `true` if there were any side effects.
    fn union(&mut self, state: &mut S, other: &Self) -> bool;
}

/// Convenience methods for nodes.
pub trait NodeExt: Node {
    /// Maps the items in this node, potentially from one type to another.
    fn map<O: Send + Sync>(
        self,
        cb: impl Fn(Self::Item) -> O + Send + Sync,
    ) -> impl Node<Item = O> {
        Map { node: self, cb }
    }

    /// Consolidates a single update batch into a running total of weight deltas.
    fn consolidate(&mut self) -> OrdMap<Self::Item, i16>
    where
        Self::Item: Clone + Ord + Send + Sync,
    {
        // TODO: use explicit im pool allocation?
        // TODO: make diff type generic (needs "one" value from num crate)
        let mut total = OrdMap::new();

        self.reduce(
            OrdMap::new,
            |subtotal, update| {
                let delta = update.delta();
                let entry = subtotal.entry(update.item).or_default();
                *entry += delta;
            },
            |subtotal| {
                total = total
                    .clone()
                    .union_with(subtotal, |total, subtotal| total + subtotal);
            },
        );

        total
    }
}

impl<N: Node> NodeExt for N {}

/// A symmetric equijoin dataflow node.
pub struct Join<L: KeyValueNode, R: KeyValueNode> {
    left: Arrange<L>,
    right: Arrange<R>,
}

impl<L, R> Node for Join<L, R>
where
    L: KeyValueNode,
    R: KeyValueNode<Key = L::Key>,
{
    type Item = (L::Key, L::Value, R::Value);

    fn reduce<T>(
        &mut self,
        begin: impl Fn() -> T + Send + Sync,
        for_each: impl Fn(&mut T, Update<Self::Item>) + Send + Sync,
        finish: impl FnMut(T) + Send + Sync,
    ) {
        self.left.join(&mut self.right, begin, for_each, finish);
    }
}

pub struct Arrange<N: KeyValueNode> {
    node: N,
    weights: OrdMap<N::Key, OrdMap<N::Value, i16>>,
}

impl<N> Arrange<N>
where
    N: KeyValueNode,
{
    pub fn join<T, R: KeyValueNode<Key = N::Key>>(
        &mut self,
        right: &mut Arrange<R>,
        begin: impl Fn() -> T + Send + Sync,
        for_each: impl Fn(&mut T, Update<(N::Key, N::Value, R::Value)>) + Send + Sync,
        mut finish: impl FnMut(T) + Send + Sync,
    ) {
        // copy current weights to observe updates in parallel
        let weights = self.weights.clone();

        // reduce right branch
        right.reduce_by_key(
            |key| {
                (
                    key.clone(),
                    weights.get(key).cloned().unwrap_or_default(),
                    begin(),
                )
            },
            |(key, left, state), update| {
                // TODO: figure out the weight diff logic (write unit tests)
                for (left, weight) in left.iter() {
                    let key = key.clone();
                    let left = left.clone();
                    let weight = *weight > 0;
                    let update = update.clone().map(|right| (key, left, right));
                    for_each(state, update);
                }
            },
            |(key, left, state), weights| {
                finish(state);
            },
        );

        // copy right weights to observe updates in parallel
        let weights = right.weights.clone();

        // reduce left branch
        self.reduce_by_key(
            |key| {
                (
                    key.clone(),
                    weights.get(key).cloned().unwrap_or_default(),
                    begin(),
                )
            },
            |(key, right, state), update| {
                // TODO: figure out the weight diff logic (write unit tests)
                for (right, weight) in right.iter() {
                    let key = key.clone();
                    let right = right.clone();
                    let weight = *weight > 0;
                    let update = update.clone().map(|left| (key, left, right));
                    for_each(state, update);
                }
            },
            |(key, left, state), weights| {
                finish(state);
            },
        );
    }

    pub fn reduce_by_key<T>(
        &mut self,
        begin: impl Fn(&N::Key) -> T + Send + Sync,
        for_each: impl Fn(&mut T, Update<N::Value>) + Send + Sync,
        finish: impl FnMut(T, &OrdMap<N::Value, i16>) + Send + Sync,
    ) {
        // retrieve all node deltas batched by key
        let delta = self.node.consolidate_by_key();

        // copy current weights to observe in parallel
        let weights = self.weights.clone();

        // wrap mutable data in mutexes
        let finish = Mutex::new(finish);
        let weights_out = Mutex::new(&mut self.weights);

        // iterate in parallel on each key at a time
        delta.into_iter().par_bridge().for_each(|(key, values)| {
            // lazily initialize iterator state
            let mut state = None;

            // retrieve the current weights
            let mut weights = weights.get(&key).cloned().unwrap_or_default();

            // retain if the current weights were empty to save diffing later
            let was_empty = weights.is_empty();

            // track if the weights were changed at all
            let mut weights_dirty = false;

            // run delta of each value against current state
            // TODO: use OrdMap::diff() instead to minimize lookups
            for (value, delta) in values {
                // if delta is 0, skip weight update
                if delta == 0 {
                    continue;
                }

                // update running weight based on delta
                weights = weights.alter(
                    |entry| {
                        // compute weights of this entry
                        let old_weight = entry.unwrap_or(0);
                        let new_weight = old_weight + delta;

                        // if weight crosses existence threshold, send update
                        if new_weight > 0 && old_weight <= 0 {
                            let state = state.get_or_insert_with(|| begin(&key));
                            let update = Update::insert(value.clone());
                            for_each(state, update);
                        } else if new_weight <= 0 && old_weight > 0 {
                            let state = state.get_or_insert_with(|| begin(&key));
                            let update = Update::remove(value.clone());
                            for_each(state, update);
                        }

                        // ensure weight dirtiness is tracked
                        weights_dirty = true;

                        // return new weight, or none if zero
                        if new_weight == 0 {
                            None
                        } else {
                            Some(new_weight)
                        }
                    },
                    value.clone(),
                );
            }

            // if state was used, finish
            if let Some(state) = state {
                let mut finish = finish.lock();
                finish(state, &weights);
            }

            // modify weights based on occupancy change
            if weights_dirty {
                if !was_empty && weights.is_empty() {
                    // remove weights if they became empty
                    weights_out.lock().remove(&key);
                } else {
                    // directly update weights otherwise
                    weights_out.lock().insert(key, weights);
                }
            }
        });
    }
}

/// A trait for nodes that collect key-value collections.
pub trait KeyValueNode: Node<Item = (Self::Key, Self::Value)> {
    /// The key stored in each node value.
    type Key: Clone + Ord + Send + Sync + 'static;

    /// The value stored in each node value.
    type Value: Clone + Ord + Send + Sync;

    /// Joins this node against another key-value node.
    fn join<O>(self, other: O) -> impl Node<Item = (Self::Key, Self::Value, O::Value)>
    where
        O: KeyValueNode<Key = Self::Key>,
    {
        Join {
            left: self.arrange(),
            right: other.arrange(),
        }
    }

    /// Arranges this node by key.
    fn arrange(self) -> Arrange<Self> {
        Arrange {
            node: self,
            weights: Default::default(),
        }
    }

    /// Consolidates a single update batch into a running total of weight deltas,
    /// grouped by their keys.
    fn consolidate_by_key(&mut self) -> OrdMap<Self::Key, OrdMap<Self::Value, i16>>
    where
        Self::Item: Clone + Ord + Send + Sync,
    {
        // TODO: use explicit im pool allocation?
        // TODO: make diff type generic (needs "one" value from num crate)
        let mut total = OrdMap::new();

        self.reduce(
            OrdMap::<Self::Key, OrdMap<Self::Value, _>>::new,
            |subtotal, update| {
                let delta = update.delta();
                let (key, value) = update.item;
                let weight = subtotal.entry(key).or_default().entry(value).or_default();
                *weight += delta;
            },
            |subtotal| {
                total = total.clone().union_with(subtotal, |total, subtotal| {
                    total.union_with(subtotal, |total, subtotal| total + subtotal)
                });
            },
        );

        total
    }
}

impl<N, K, V> KeyValueNode for N
where
    N: Node<Item = (K, V)>,
    K: Clone + Ord + Send + Sync + 'static,
    V: Clone + Ord + Send + Sync,
{
    type Key = K;
    type Value = V;
}

pub struct Map<N, F> {
    node: N,
    cb: F,
}

impl<N, F, O> Node for Map<N, F>
where
    N: Node,
    F: Fn(N::Item) -> O + Send + Sync,
    O: Send + Sync,
{
    type Item = O;

    fn reduce<T>(
        &mut self,
        begin: impl Fn() -> T + Send + Sync,
        for_each: impl Fn(&mut T, Update<O>) + Send + Sync,
        finish: impl FnMut(T) + Send + Sync,
    ) {
        self.node.reduce(
            begin,
            |state, update| for_each(state, update.map(&self.cb)),
            finish,
        );
    }
}

/// A dataflow node that provides input to a dataflow using an [Input] channel.
pub struct Origin<T> {
    rx: Receiver<Input<T>>,
}

impl<T> FromIterator<Update<T>> for Origin<T> {
    fn from_iter<I: IntoIterator<Item = Update<T>>>(iter: I) -> Self {
        let (tx, rx) = flume::unbounded();

        for item in iter.into_iter().map(Input::Update) {
            tx.send(item).unwrap();
        }

        Self { rx }
    }
}

impl<T> Origin<T> {
    /// Create an origin from an input event receiver.
    pub fn new(rx: Receiver<Input<T>>) -> Self {
        Self { rx }
    }

    /// Creates an origin from a set of initial contents.
    pub fn from_initial(iter: impl IntoIterator<Item = T>) -> Self {
        Self::from_iter(iter.into_iter().map(Update::insert))
    }

    /// Receives an input update while cooperatively executing Rayon tasks.
    fn recv_and_yield(&self) -> Input<T> {
        loop {
            // poll for an input update
            use flume::TryRecvError;
            match self.rx.try_recv() {
                // if input was found, yield it
                Ok(input) => return input,
                // receiving from closed origins yields empty batches forever
                Err(TryRecvError::Disconnected) => return Input::Flush,
                // fall through to rayon work
                Err(TryRecvError::Empty) => {}
            }

            // cooperatively pick up Rayon tasks
            use rayon::Yield;
            match yield_now() {
                // if Rayon work was done, poll and repeat
                Some(Yield::Executed) => continue,
                // if this thread is not part of a Rayon pool, receive blocking
                None => {}
                // if there is no Rayon work left, receive blocking
                Some(Yield::Idle) => {}
            }

            // blocking receive an input update
            use flume::RecvError;
            match self.rx.recv() {
                // if input was found, yield it
                Ok(input) => return input,
                // closed origins yields empty batches forever
                Err(RecvError::Disconnected) => return Input::Flush,
            }
        }
    }
}

impl<I: Send + Sync> Node for Origin<I> {
    type Item = I;

    fn reduce<T>(
        &mut self,
        begin: impl Fn() -> T + Send + Sync,
        for_each: impl Fn(&mut T, Update<Self::Item>) + Send + Sync,
        mut finish: impl FnMut(T) + Send + Sync,
    ) {
        let mut state = begin();

        while let Input::Update(update) = self.recv_and_yield() {
            for_each(&mut state, update);
        }

        finish(state);
    }
}

/// An input event for a dataflow.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Input<T> {
    /// An item in this dataflow is being updated.
    Update(Update<T>),

    /// This input batch has concluded.
    Flush,
}

/// The base trait for dataflow nodes.
pub trait Node: Sized + Send + Sync {
    /// The type of items collected in this node.
    type Item;

    fn reduce<T>(
        &mut self,
        begin: impl Fn() -> T + Send + Sync,
        for_each: impl Fn(&mut T, Update<Self::Item>) + Send + Sync,
        finish: impl FnMut(T) + Send + Sync,
    );
}

/// A single differential (insert/remove) update for an item.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Update<T> {
    /// The item.
    pub item: T,

    /// `true` for insertion, `false` for removal.
    pub weight: bool,
}

impl<T> Update<T> {
    pub fn map<O>(self, cb: impl FnOnce(T) -> O) -> Update<O> {
        Update {
            item: cb(self.item),
            weight: self.weight,
        }
    }

    pub fn insert(item: T) -> Self {
        Self { item, weight: true }
    }

    pub fn remove(item: T) -> Self {
        Self {
            item,
            weight: false,
        }
    }

    pub fn delta(&self) -> i16 {
        if self.weight { 1 } else { -1 }
    }

    pub fn unit_delta_map(self) -> OrdMap<T, i16> {
        let delta = self.delta();
        OrdMap::unit(self.item, delta)
    }
}

impl<K, V> Update<(K, V)> {
    pub fn unit_delta_map_keyed(self) -> OrdMap<K, OrdMap<V, i16>> {
        let delta = self.delta();
        let (key, value) = self.item;
        OrdMap::unit(key, OrdMap::unit(value, delta))
    }
}
