# Undyne

Differential Dataflow is fantastic, but it's also overly complicated if you just
want to quickly define some local-only semi-naive relational logic. Something
it does extremely well, however, is its API for constructing type-safe dataflow
nodes. This project is an attempt at implementing efficient, differential,
semi-naive relational logic (with iterative loops!) using the Rust type system
(combinators) and without using a complex transport mechanism for clustered
computing (Timely).

A main architectural decision is to use Rayon for parallelism rather than using
explicit worker pools. Although this does not take advantage of Differential
Dataflow's use of hash addressing to efficiently partition work between workers,
this greatly simplifies the implementation.
