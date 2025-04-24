//客户端服务代理，用于管理和协调与基于BRPC的Raft集群的通信
//这是指一个在客户端和服务器之间充当中介的组件或服务。它负责接收客户端的请求，然后将这些请求转发给服务器或集群中的合适节点。这个代理可以实现负载均衡、请求路由、错误处理等功能
//BRPC（Baidu RPC）：这是百度开发的高性能远程过程调用（RPC）框架，允许不同计算机之间进行通信。它是用来构建分布式系统的通信基础框架
//通过RPC，开发者可以像调用本地函数一样，调用远程服务器上的服务，而不需要关心底层的网络通信细节
package com.github.raftimpl.raft.admin;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.instance.Endpoint;
import com.github.raftimpl.raft.proto.RaftProto;
import com.github.raftimpl.raft.service.RaftClientService;
import com.googlecode.protobuf.format.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RaftClientServiceProxy implements RaftClientService {
    private static final Logger LOG = LoggerFactory.getLogger(RaftClientServiceProxy.class);
    private static final JsonFormat jsonFormat = new JsonFormat();

    private List<RaftProto.Server> cluster;
    private RpcClient clusterRPCClient;
    private RaftClientService clusterRaftClientService;

    private RaftProto.Server leader;
    private RpcClient leaderRPCClient;
    private RaftClientService leaderRaftClientService;

    private RpcClientOptions rpcClientOptions = new RpcClientOptions();

    // servers format is 10.1.1.1:8888,10.2.2.2:9999
    public RaftClientServiceProxy(String ipPorts) {
        rpcClientOptions.setConnectTimeoutMillis(1000); // 1s
        rpcClientOptions.setReadTimeoutMillis(3600000); // 1hour
        rpcClientOptions.setWriteTimeoutMillis(1000); // 1s
        clusterRPCClient = new RpcClient(ipPorts, rpcClientOptions);
        clusterRaftClientService = BrpcProxy.getProxy(clusterRPCClient, RaftClientService.class);
        updateConfiguration();
    }

    @Override
    public RaftProto.GetLeaderResponse getLeader(RaftProto.GetLeaderRequest request) {
        return clusterRaftClientService.getLeader(request);
    }

    @Override
    public RaftProto.GetConfigurationResponse getConfiguration(RaftProto.GetConfigurationRequest request) {
        return clusterRaftClientService.getConfiguration(request);
    }

    @Override
    public RaftProto.AddPeersResponse addPeers(RaftProto.AddPeersRequest request) {
        RaftProto.AddPeersResponse response = leaderRaftClientService.addPeers(request);
        if (response != null && response.getResCode() == RaftProto.ResCode.RES_CODE_NOT_LEADER) {
            updateConfiguration();
            response = leaderRaftClientService.addPeers(request);
        }
        return response;
    }

    @Override
    public RaftProto.RemovePeersResponse removePeers(RaftProto.RemovePeersRequest request) {
        RaftProto.RemovePeersResponse response = leaderRaftClientService.removePeers(request);
        if (response != null && response.getResCode() == RaftProto.ResCode.RES_CODE_NOT_LEADER) {
            updateConfiguration();
            response = leaderRaftClientService.removePeers(request);
        }
        return response;
    }

    public void stop() {
        if (leaderRPCClient != null) {
            leaderRPCClient.stop();
        }
        if (clusterRPCClient != null) {
            clusterRPCClient.stop();
        }
    }

    private boolean updateConfiguration() {
        RaftProto.GetConfigurationRequest request = RaftProto.GetConfigurationRequest.newBuilder().build();
        RaftProto.GetConfigurationResponse response = clusterRaftClientService.getConfiguration(request);
        if (response != null && response.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {
            if (leaderRPCClient != null) {
                leaderRPCClient.stop();
            }
            leader = response.getLeader();
            leaderRPCClient = new RpcClient(convertEndPoint(leader.getEndpoint()), rpcClientOptions);
            leaderRaftClientService = BrpcProxy.getProxy(leaderRPCClient, RaftClientService.class);
            return true;
        }
        return false;
    }

    private Endpoint convertEndPoint(RaftProto.Endpoint endPoint) {
        return new Endpoint(endPoint.getHost(), endPoint.getPort());
    }

}
