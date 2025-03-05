package com.github.raftimpl.raft.service;

import com.baidu.brpc.client.RpcCallback;
import com.github.raftimpl.raft.proto.RaftProto;

import java.util.concurrent.Future;

/**
 * 用于生成client异步调用所需的proxy
 * Created by raftimpl on 2017/5/2.
 */

//Extends RaftConsensusService to provide asynchronous (non-blocking) versions of the methods.
//Methods: Each method returns a Future representing the pending result and accepts a RpcCallback for handling the response asynchronously.
//Execution Model: The caller invokes a method and continues execution without blocking, with the response handled via a callback when it's available.
public interface RaftConsensusServiceAsync extends RaftConsensusService {

    Future<RaftProto.VoteResponse> preVote(//Returns a Future<RaftProto.VoteResponse>, representing the pending result of the asynchronous operation.
            RaftProto.VoteRequest request,
            RpcCallback<RaftProto.VoteResponse> callback);

    Future<RaftProto.VoteResponse> requestVote(
            RaftProto.VoteRequest request,
            RpcCallback<RaftProto.VoteResponse> callback);

    Future<RaftProto.AppendEntriesResponse> appendEntries(
            RaftProto.AppendEntriesRequest request,
            RpcCallback<RaftProto.AppendEntriesResponse> callback);

    Future<RaftProto.InstallSnapshotResponse> installSnapshot(
            RaftProto.InstallSnapshotRequest request,
            RpcCallback<RaftProto.InstallSnapshotResponse> callback);
}
