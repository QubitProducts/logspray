// Package indexer processing logs and stores them for
// querying. This is probably confusing several concerns
// and needs work.
//
// On start up, each indexer is assigned a new UUID.
// The indexer creates shards, collections of shard files that span a liited
// time duration (shardDuration). Each shard created by an indexer is assigned
// a UUID.
// Each inoming stream is assigned a UUID. A sharfile is created for each stream.
// Each message in a stream is assigned its ordinal position in the stream
//
// The dataDir should be laid out as follows:
// dataDir/shardID/streamID.pb.log
package indexer
