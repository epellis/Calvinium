# Calvinium

Calvinium is a Work In Progress (WIP) Distributed SQL Database based on
the [Calvin](http://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf) paper.

## TODO List

- [x] Read and Write on single partition, single replica
- [x] Read and Write on single partition, multiple replica
- [x] Read and Write on multiple partition, single replica
- [x] Read and Write on multiple partition, multiple replica
- [x] Implement raft log or use OSS library
- [ ] Online partition scaling
- [ ] Integration test for strong transaction consistency
- [ ] `CREATE TABLE` and proper support for simple data types
- [ ] ANSI SQL Support
- [ ] SQL Selects on a non primary key (i.e. reconnaissance queries)
- [ ] Pass [TPC-C](https://tpc.org/tpcc/default5.asp)
- [ ] Application level "chaos testing" framework
- [ ] GRPC Web Client
- [ ] Go Database Driver
- [ ] S3 Log Streaming for backups

## Aspirations

- Application level multitenancy (i.e. strong isolation without an OS level VM)
- In addition to SQL, support user specified transaction logic via scripting language (e.g. JS, Lua, ...)
- Integration with [Jepsen](https://github.com/jepsen-io/jepsen) model checker
- Allows number of partitions and number of replicas to be changed while remaining available
- [Open Tracing](https://opentracing.io/) integration
- Builtin online backup that streams logfiles to S3 (ala [Litestream](https://litestream.io/))
- HTTP/1.1 client using GRPC Web
- First party Kubernetes operator

## Testing 

To access debug logs, run test with `--info`: `./gradlew test --info`

## Logging

Logging is configured in `src/test/resources/simplelogger.properties`.


## Formatting

This project use [ktfmt](https://github.com/facebookincubator/ktfmt). You can
run the formatter via: `./gradlew ktfmtFormat`

## Components

- Leadership election and state replication via [Raft](https://raft.github.io/raft.pdf)
- Membership via [Gossip Failure Detector](https://www.cs.cornell.edu/home/rvr/papers/GossipFD.pdf)
