use flume::Receiver;
use im::{OrdMap, OrdSet};
use parking_lot::Mutex;
use rayon::{prelude::*, yield_now};

#[cfg(test)]
pub mod tests;

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

/// An input event for a dataflow.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Input<T> {
    /// An item in this dataflow is being updated.
    Update(Update<T>),

    /// This input batch has concluded.
    Flush,
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

impl<T: Send + Sync> Node for Origin<T> {
    type Item = T;

    fn update(&mut self) -> impl ParallelIterator<Item = Update<Self::Item>> + '_ {
        // create parallel iterator that yields a single batch of updates
        std::iter::from_fn(|| match self.recv_and_yield() {
            Input::Update(update) => Some(update),
            Input::Flush => None,
        })
        .par_bridge()
    }
}

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

/// The base trait for dataflow nodes.
pub trait Node: Sized + Send + Sync {
    /// The type of items collected in this node.
    type Item;

    /// Iterates over a whole batch of updates to this node.
    fn update(&mut self) -> impl ParallelIterator<Item = Update<Self::Item>> + '_;
}

/// Convenience methods for nodes.
pub trait NodeExt: Node {
    /// Maps the items in this node.
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
        self.update()
            .map(Update::unit_delta_map)
            .reduce(OrdMap::new, |left, right| {
                left.union_with(right, |left, right| left + right)
            })
    }
}

impl<N: Node> NodeExt for N {}

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

    fn update(&mut self) -> impl ParallelIterator<Item = Update<Self::Item>> + '_ {
        self.node.update().map(|update| update.map(&self.cb))
    }
}

/// A trait for nodes that collect key-value collections.
pub trait KeyValueNode: Node<Item = (Self::Key, Self::Value)> {
    /// The key stored in each node value.
    type Key: Clone + Ord + Send + Sync + 'static;

    /// The value stored in each node value.
    type Value: Clone + Ord + Send + Sync;
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

/// Convenience methods for key-value nodes.
pub trait KeyValueNodeExt: KeyValueNode {
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
    fn arrange(self) -> impl Arranged<Key = Self::Key, Value = Self::Value> {
        Arrangement {
            node: self,
            state: Mutex::new(ArrangedState::new()),
            weights: Mutex::new(OrdMap::new()),
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
        self.update()
            .map(Update::unit_delta_map_keyed)
            .reduce(OrdMap::new, |left, right| {
                left.union_with(right, |left, right| {
                    left.union_with(right, |left, right| left + right)
                })
            })
    }
}

impl<N> KeyValueNodeExt for N where N: KeyValueNode {}

/// An arranged collection of key-value relations.
pub trait Arranged: Send + Sync {
    /// The key stored in each node value.
    type Key: Clone + Ord + Send + Sync;

    /// The value stored in each node value.
    type Value: Clone + Ord + Send + Sync;

    /// Gets the current state of this arrangement.
    fn state(&self) -> ArrangedState<Self::Key, Self::Value>;

    /// Iterates over the updates in this arrangement while updating state.
    fn update(&mut self) -> impl ParallelIterator<Item = Update<(Self::Key, Self::Value)>> + '_;
}

pub type ArrangedState<K, V> = OrdMap<K, OrdSet<V>>;

/// An arrangement: a reduced collection of key-value relations.
pub struct Arrangement<N, K, V> {
    node: N,
    state: Mutex<ArrangedState<K, V>>,
    weights: Mutex<OrdMap<K, OrdMap<V, i16>>>,
}

impl<N, K, V> Arranged for Arrangement<N, K, V>
where
    N: KeyValueNode<Key = K, Value = V>,
    K: Ord + Clone + Send + Sync,
    V: Ord + Clone + Send + Sync,
{
    type Key = K;
    type Value = V;

    fn state(&self) -> ArrangedState<K, V> {
        self.state.lock().clone()
    }

    fn update(&mut self) -> impl ParallelIterator<Item = Update<(K, V)>> {
        self.node
            .consolidate_by_key()
            .into_iter()
            .par_bridge()
            .flat_map_iter(move |(key, values)| {
                // clone current state and running weights
                let mut weights = self.weights.lock().get(&key).cloned().unwrap_or_default();
                let mut state = self.state.lock().get(&key).cloned().unwrap_or_default();

                // apply weight deltas while collecting state diffs
                let mut diff = Vec::with_capacity(values.len());
                for (value, delta) in values {
                    weights = weights.alter(
                        |entry| {
                            let old_value = entry.unwrap_or(0);
                            let new_value = old_value + delta;

                            if new_value > 0 && old_value <= 0 {
                                state.insert(value.clone());
                                diff.push(Update::insert((key.clone(), value.clone())));
                            } else if new_value <= 0 && old_value > 0 {
                                state.remove(&value);
                                diff.push(Update::remove((key.clone(), value.clone())));
                            }

                            if new_value == 0 {
                                None
                            } else {
                                Some(new_value)
                            }
                        },
                        value.clone(),
                    );
                }

                // update running weights
                self.weights.lock().insert(key.clone(), weights);

                // update running state
                self.state.lock().insert(key, state);

                // return update diff
                diff
            })
    }
}

/// A symmetric equijoin dataflow node.
pub struct Join<L, R> {
    left: L,
    right: R,
}

impl<K, L, R> Node for Join<L, R>
where
    K: Clone + Ord + Send + Sync + 'static,
    L: Arranged<Key = K>,
    R: Arranged<Key = K>,
{
    type Item = (K, L::Value, R::Value);

    fn update(&mut self) -> impl ParallelIterator<Item = Update<Self::Item>> + '_ {
        // update left branch and preserve updates
        let left_updates = self.left.update().collect_vec_list();

        // join old right state against batched left updates
        // TODO: would manual rayon consumers be more efficient than collecting updates? BENCH FIRST
        let right_half = half_join(
            self.right.state(),
            left_updates.into_iter().par_bridge().flatten(),
        );

        // join right updates against up-to-date left state
        let left_half = half_join(self.left.state(), self.right.update());

        // combine halves
        right_half
            .map(|update| update.map(|(key, right, left)| (key, left, right)))
            .chain(left_half)
    }
}

// TODO: rename to inner join? split join?
pub(crate) fn half_join<'a, K, VL, VR>(
    state: ArrangedState<K, VL>,
    updates: impl ParallelIterator<Item = Update<(K, VR)>> + 'a,
) -> impl ParallelIterator<Item = Update<(K, VL, VR)>> + 'a
where
    K: Clone + Ord + Send + Sync + 'a,
    VL: Clone + Ord + Send + Sync + 'a,
    VR: Clone + Send + Sync + 'a,
{
    updates
        .flat_map_iter(move |item| {
            let Update {
                item: (key, outer),
                weight,
            } = item;

            state
                .get(&key)
                .cloned()
                .map(|inner| (key, weight, inner, outer))
        })
        .flat_map_iter(move |(key, weight, inner, outer)| {
            inner.into_iter().map(move |inner| Update {
                weight,
                item: (key.clone(), inner.to_owned(), outer.clone()),
            })
        })
}
