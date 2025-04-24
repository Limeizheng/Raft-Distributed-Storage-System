package com.github.raftimpl.raft.service;

import com.github.raftimpl.raft.proto.RaftProto;

/**
 * raft节点之间相互通信的接口。
 */

//Defines the synchronous (blocking) methods for Raft consensus operations
//The caller invokes a method and waits (blocks) until the method completes and returns a response.
public interface RaftConsensusService {

    RaftProto.VoteResponse preVote(RaftProto.VoteRequest request);

    RaftProto.VoteResponse requestVote(RaftProto.VoteRequest request);

    RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request);

    RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request);
}
