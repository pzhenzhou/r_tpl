## Just TPL Protocol Simulator

### How to build

The compile and build process relies on the cargo-make, which needs to be pre-installed

- Install cargo-make

```
cargo install --force cargo-make
```

- Execute the following command to build. build and testing.

```
cargo make --makefile cargo-make.toml all-flow
```

### How to run

```
cargo build --release
cd target/release
./r_tpl
```

### Design

1. Abstraction
    - DBObject: The relationship is similar between Segment/Chunk/Tuple and Database/Table/Tuple, and this abstraction
      is more beneficial for MGL.
    - LockManager： There is no state to handle the actual TPL protocol, e.g., lock compatibility, whether locks can be
      promoted, and there should be another abstraction in the actual scenario such as LockManagerWrapper/LockContext to
      handle MGL ( Parent is locked or not)
    - LockTable： Recording the mapping between Operation/Resource/Lock, thread-safe can be shared globally.
2. Design Considerations
    - Lock granularity
        1. Chunk-based locking, when the system will have a fixed number of locks (the granularity of MySQL page-level
           locking), because the amount of data locked at a time is large, so for long transactions friendly, but
           latency-sensitive applications is not good.
        2. Based on resource-specific locks, the LockManager allocates and reclaims locks frequently. However, locks are
           more fine-grained.
3. TODO
    1. LockTable holds a RwLock with bad performance
    2. When a lock incompatibility is detected, it should wait until another operation releases the resource instead of
       returning an error
    3. Deadlock handling is actually traded off in practical application scenarios. timeout-based mechanisms are a very
       simple and practical approach, and can also be
       used [Thomas write rule](https://en.wikipedia.org/wiki/Thomas_write_rule)

4. Reference

   [CMU TPL Course](https://15445.courses.cs.cmu.edu/fall2019/slides/17-twophaselocking.pdf)