# LogSpray

This is a gRPC service, and clients, for distributing and
tailing logs. Logs are forwarded by a server side agent (can be
deployed on each node, or as a daemnset in kubernetes).

# Work in progrss.

This code has been running in production at Qubit for some time. The basic
functionality has proved useful, allow developers to stream log content from
all running instances of their applications.

Implementation of search functionality has changed over time and is currently

This code is very much a work in progress. The content has been migrated from
an internal repository, to this public repository for the purpose of releasing
to the public. There may be some rough edges around the build due to the move.

In particular:

- The user facing tail/search logclient needs work after the move.
- Not dependency requirements, will be provided via dep
- The helm configuration needs updating.

There is still much to do in the code:

- The proto definitions need revision.
- reader/server should probably move to a single binary (cobra?)
- reader config is currently hardcoded.
- The index is not reopulated from disk, only the last
  15m of data can be search at present.
- indexer/archive needs work.
- Code for retrieiving a jwt token is missing from the
  reader (internal system provides this at present).
- The ql package is a work in progress.

# OK, so what actually works?

- gRPC streaming of labeled logs from all active docker containers.
- RESTful streaming of log data (via grpc-gateway)
- Streaming of file content (though is is not heavily tested)
- Translation of environment variables and container labels to log labels.
- Search based on simple label matching (regex code exists, but is not
  surfaced to the user at this time.
- Integration with grafana-simple-json
- Server side grep of streaming content (to avoid wasted bandwith)

