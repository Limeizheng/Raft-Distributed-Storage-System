package com.github.raftimpl.raft.example.server.machine;

import com.github.raftimpl.raft.StateMachine;
import com.github.raftimpl.raft.example.server.service.ExampleProto;
import org.apache.commons.io.FileUtils;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by raftimpl on 2017/5/9.
 */
public class RocksDBStateMachine implements StateMachine {
    // log记录日志
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateMachine.class);
    // 静态代码块，项目启动时执行，用于初始化RocksDB，加载RocksDB依赖包
    static {
        RocksDB.loadLibrary();
    }

    private RocksDB db;// RocksDB数据库
    private final String raftDataDir;// 节点数据存放路径
    // 构造方法，从外部传入数据存放路径
    public RocksDBStateMachine(String raftDataDir) {
        this.raftDataDir = raftDataDir;
    }

    /**
     * StateMachine接口中的方法重写，实现snapShot写入
     * @param snapshotDir 旧snapshot目录
     * @param tmpSnapshotDataDir 新snapshot数据目录
     * @param raftNode Raft节点
     * @param localLastAppliedIndex 已应用到复制状态机的最大日志条目索引
     */

    @Override
    public void writeSnapshot(String snapshotDir) {
        Checkpoint checkpoint = Checkpoint.create(db);// 构造RocksDB的Checkpoint
        try {
            checkpoint.createCheckpoint(snapshotDir);
        } catch (Exception e) {
            LOG.warn("writeSnapshot meet exception, dir={}, msg={}",
                    snapshotDir, e.getMessage());
        }
    }
    /**
     * StateMachine接口中的方法重写，实现snapShot读取
     * @param snapshotDir snapshot数据目录
     */

    @Override
    public void readSnapshot(String snapshotDir) {
        try {
            // copy snapshot dir to data dir
            if (db != null) {// 因为要做文件夹复制，所以先关闭数据库连接
                db.close();
                db = null;
            }
            // 节点存放rocksdb data路径
            String dataDir = raftDataDir + File.separator + "rocksdb_data";
            File dataFile = new File(dataDir);
            if (dataFile.exists()) {
                FileUtils.deleteDirectory(dataFile);
            }
            File snapshotFile = new File(snapshotDir);
            // 将snapshot下的文件复制到dataDir下
            if (snapshotFile.exists()) {
                FileUtils.copyDirectory(snapshotFile, dataFile);
            }
            // open rocksdb data dir// 重新打开数据库连接
            Options options = new Options();
            options.setCreateIfMissing(true);
            db = RocksDB.open(options, dataDir);
        } catch (Exception e) {
            LOG.warn("meet exception, msg={}", e.getMessage());
        }
    }

    @Override
    public void apply(byte[] dataBytes) {
        try {
            ExampleProto.SetRequest request = ExampleProto.SetRequest.parseFrom(dataBytes);
            db.put(request.getKey().getBytes(), request.getValue().getBytes());
        } catch (Exception e) {
            LOG.warn("meet exception, msg={}", e.getMessage());
        }
    }

    @Override
    public byte[] get(byte[] dataBytes) {
        byte[] result = null;
        try {
            byte[] valueBytes = db.get(dataBytes);
            if (valueBytes != null) {
                result = valueBytes;
            }
        } catch (Exception e) {
            LOG.warn("read rocksdb error, msg={}", e.getMessage());
        }
        return result;
    }

}

