//客户端应用程序，用于执行基本的键值存储操作
package com.github.raftimpl.raft.example.client;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.github.raftimpl.raft.example.server.service.ExampleProto;
import com.github.raftimpl.raft.example.server.service.ExampleService;
import com.googlecode.protobuf.format.JsonFormat;

//客户端应用程序，用于执行基本的键值存储操作
public class ClientMain {
    public static void main(String[] args) {
        /*if (args.length < 2) {
            System.out.printf("Usage: ./run_client.sh CLUSTER KEY [VALUE]\n");
            System.exit(-1);
        }

        // parse args
        String ipPorts = args[0];
        String key = args[1];
        String value = null;
        if (args.length > 2) {
            value = args[2];
        }*/

        String ipPorts = "list://192.168.227.117:8051,192.168.227.117:8052,192.168.227.117:8053";
        String key = "hello";
        String value = null;

        // init rpc client
        RpcClient rpcClient = new RpcClient(ipPorts);
        ExampleService exampleService = BrpcProxy.getProxy(rpcClient, ExampleService.class);
        final JsonFormat jsonFormat = new JsonFormat();

        // set
        if (value != null) {
            ExampleProto.SetRequest setRequest = ExampleProto.SetRequest.newBuilder()
                    .setKey(key).setValue(value).build();
            ExampleProto.SetResponse setResponse = exampleService.set(setRequest);
            System.out.printf("set request, key=%s value=%s response=%s\n",
                    key, value, jsonFormat.printToString(setResponse));
        } else {
            // get
            ExampleProto.GetRequest getRequest = ExampleProto.GetRequest.newBuilder()
                    .setKey(key).build();
            ExampleProto.GetResponse getResponse = exampleService.get(getRequest);
            System.out.printf("get request, key=%s, response=%s\n",
                    key, jsonFormat.printToString(getResponse));
        }

        rpcClient.stop();
    }
}
