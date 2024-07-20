# Dev notes on parallelization of access to databases

These notes are based on FDBChainStore but will be applicable to many different resource types that involve waiting for results.
For example accessing network resources (e.g. databases, remote file stores) or local disks.
The context is Rust async, in particular the tokio library.

We want to enable massive parallelization of queries to the database. Modern database technologies excel at
simultaneously processing many queries. If a single application continually executes one query at a time then
this is a severe limitation on the applications throughput. This is clearly visible in the system performance monitoring:
an application that is continuously executing database queries will have a low use of local system resources as it 
continually waits for answers from the database.

Our design must enable execution of many queries simultaneously.

## Cloning the database interface

The first element is cloning the database interface (in our case, FDBChainStore). We want to be able to use functionality like this:
```
    let db = FDBChainStore::new();
    ...
    let db2 = db.clone();
    let j = tokio::spawn(async move || {
            let i = db2.get_data("param").await;
            // do something with i
        });
```
and we want to be able to spawn several of these tasks at the same time. We need to clone `db` into `db2` so that it can be
moved into the closure which is spawned as a separate task. But we dont want to create an entirely new connection to the underlying
database to do this, that would be too heavy, we want a very light and fast mechanism to do this.

This is where the Actor model comes in. In my design, the FDBChainStore is merely a small stub that communicates with the
background actor (FDBChainStoreActor). The FDBChainStore struct is small and contains the sending end of a 
multi-producer-single-consumer (MPSC) channel. The methods on this struct (e.g. get_data) send a message via the channel to
the actor at the receiving end and waits for a result. The actor processes the messages it receives (potentially from many stubs) and 
responds with the results. We can clone the FDBChainStore stub many times and each clone will interact with the same background actor.

The message that is sent to the actor also contains a new tokio::sync::oneshot channel which is used to send the result from the actor
back to the stub. Internally, the actor creates a new task which executes the query and transmits the result back to the channel.
The actor is therefore able to create many of these tasks as it receives messages from many stubs, leading to high levels of 
multi-processing.

## Executing many queries simultaneously

The next element is to be able to execute many queries at once from a single task. We need to be able to write something like this:
```
    let db = FDBChainStore::new();
    ...
    let k = db.get_data("param2").await;    // simple case, single query
    
    let mut v = vec![];
    for _ in 1..10 {
        let i = db.get_data("param");   // get the future which will give the result
        let j = tokio::spawn(i);        // spawn a separate task to perform the query
        v.push(j);                      // save the join handle for later
    }
    
    // 10 queries are now running in parallel
    
    // collect all the results
    while let Some(j) = v.pop() {
        let r = j.await.unwrap();
        ... do something with the result
    }
```

In order to be able to compile the first loop (the for loop), the future that is returned by the `get_data` call has to
be completely disassociated with the `db` struct. If it retains a reference to the `db` struct then the following iterations of 
the for loop will not be able to also obtain a reference. So, the `get_data` method has to return a closure that does not
reference the `db` struct. See for example FDBChainStore::get_block_info().
