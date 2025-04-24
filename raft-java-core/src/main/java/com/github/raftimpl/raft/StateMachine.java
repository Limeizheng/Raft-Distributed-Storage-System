//Raft状态机接口类
package com.github.raftimpl.raft;

/**
 * Raft状态机接口类
 */
public interface StateMachine {

    /**
     * 对状态机中数据进行snapshot，每个节点本地定时调用
     * @param snapshotDir snapshot数据输出目录
     */
    void writeSnapshot(String snapshotDir);

    /**
     * 读取snapshot到状态机，节点启动时调用
     * @param snapshotDir snapshot数据目录
     */
    void readSnapshot(String snapshotDir);

    /**
     * 将数据应用到状态机
     * @param dataBytes 数据二进制
     */
    void apply(byte[] dataBytes);

    /**
     * 从状态机读取数据
     * @param dataBytes Key的数据二进制
     * @return Value的数据二进制
     */
    byte[] get(byte[] dataBytes);
}
