//Raft集群中的其他节点
package com.github.raftimpl.raft;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.instance.Endpoint;
import com.github.raftimpl.raft.proto.RaftProto;
import com.github.raftimpl.raft.service.RaftConsensusServiceAsync;

/**
 * Created by raftimpl on 2017/5/5.
 */
//在Raft协议实现中代表集群中的一个节点，主要用于维护与该节点的通信和状态信息
public class Peer {
    private RaftProto.Server server;//存储了该节点的基本信息，如IP地址和端口
    private RpcClient rpcClient;//用于与远程节点进行通信的RPC客户端
    private RaftConsensusServiceAsync raftConsensusServiceAsync;//异步接口，用于执行Raft协议的操作，如日志复制和心跳
    private long nextIndex;// 需要发送给follower的下一个日志条目的索引值，只对leader有效
    private long matchIndex;// 已复制日志的最高索引值
    private volatile Boolean voteGranted;// 表示该节点是否给当前节点投了票。
    private volatile boolean isCatchUp;// 表示该节点是否已经追上了leader的日志

    public Peer(RaftProto.Server server) {
        this.server = server;
        this.rpcClient = new RpcClient(new Endpoint(
                server.getEndpoint().getHost(),
                server.getEndpoint().getPort()));
        raftConsensusServiceAsync = BrpcProxy.getProxy(rpcClient, RaftConsensusServiceAsync.class);
        isCatchUp = false;
    }

    public RaftProto.Server getServer() {
        return server;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }

    public RaftConsensusServiceAsync getRaftConsensusServiceAsync() {
        return raftConsensusServiceAsync;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    public Boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(Boolean voteGranted) {
        this.voteGranted = voteGranted;
    }


    public boolean isCatchUp() {
        return isCatchUp;
    }

    public void setCatchUp(boolean catchUp) {
        isCatchUp = catchUp;
    }
}
