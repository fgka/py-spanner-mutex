# Cloud Spanner based mutex

This implements a [Cloud Spanner](https://cloud.google.com/spanner) based distributed [mutex](https://en.wikipedia.org/wiki/Mutual_exclusion).
The main goal is to allow, for instance, to have a critical section when deploying a [Cloud Run](https://cloud.google.com/run) with multiple minimum instances.

# An obvious use case (aka why we did it)

The main motivation is to allow Cloud Run with multiple minimum instances to implement, in code, [database schema migrations](https://en.wikipedia.org/wiki/Schema_migration).
(If you are not familiar with the concept, this is discussed by [Martin Fowler](https://en.wikipedia.org/wiki/Martin_Fowler_(software_engineer)) in his blog post [Evolutionary Database Design](https://martinfowler.com/articles/evodb.html).)
To avoid having to implement [Paxos](https://people.cs.rutgers.edu/~pxk/417/notes/paxos.html) we leverage Cloud Spanner to do our bidding.

> :hand: If you are not interested in theoretical *gibberish* you can skip directly to [Show Me!](./README.md#show-me)

## [Correctness disclaimer](./code/CORRECTNESS_DISCLAIMER.md)

This solution is suitable for most use-cases.
However, there are edge cases that can make it fail and you should be aware.

## Can it work?

Yes it can (_sorry_ for the pun) because of a couple of guarantees from Cloud Spanner:
* [Read-write transactions](https://cloud.google.com/spanner/docs/transactions#rw_transaction_properties)
* [TrueTime and external consistency](https://cloud.google.com/spanner/docs/true-time-external-consistency)

The above guarantees that if your read-write transaction was committed the data is not trumped by other transactions (which will fail).

The second part, which is central to the logic, is conditional upsert (the ``insert_or_update`` Spanner [Mutation](https://cloud.google.com/spanner/docs/reference/rpc/google.spanner.v1#google.spanner.v1.Mutation)).
By conditional, we mean the following (pseudo-and-bad-code):

```text
with spanner_transaction() as txn:
  can_I_upsert = can_I_still_get_the_mutex(txn, mutex)
  if can_I_upsert:
    txn.insert_or_update(mutex)
  else:
    print("Ooops, let us skip this one.")
```

With these pieces together we can, assuming working logic, guarantee that it will work.

You can get all the details in the [design documentation](./DESIGN.md).

## Show me!

That part is easy as long as you deployed the required infrastructure and used the produced ``test_config.json``.
For details check the [terraform documentation](./terraform/README.md).

The testing instructions are in [CLI.md](./code/CLI.md).

There you will run multiple clients, all trying to acquire the critical section, with only one writing to a temporary local file.
Having a single line in the output file shows that only one client was able to acquire and execute the critical section.

## [How can i use it?](./USAGE.md)
