package com.github.raftimpl.raft.example.server.machine;

import com.github.raftimpl.raft.StateMachine;
import com.github.raftimpl.raft.example.server.service.ExampleProto;
import com.trifork.bitcask.BitCask;
import com.trifork.bitcask.BitCaskOptions;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class BitCaskStateMachine implements StateMachine {
    private static final Logger LOG = LoggerFactory.getLogger(BitCaskStateMachine.class);
    private BitCask db;
    private final String raftDataDir;//points to a directory on disk where this node stores its data files.

    public BitCaskStateMachine(String raftDataDir) {
        this.raftDataDir = raftDataDir;
    }

    @Override
    public void writeSnapshot(String snapshotDir) {
        try {
            db.close();
            db = null;
            String dataDir = raftDataDir + File.separator + "leveldb_data";
            File dataFile = new File(dataDir);

            // 检查快照目录是否存在，如果存在则清空，如果不存在则创建
            File snapshotFile = new File(snapshotDir);
            if (snapshotFile.exists() && !snapshotFile.delete()) {
                throw new IOException("error when handling data directory, please check " + snapshotDir);
            }
            if (!snapshotFile.mkdirs()) {
                throw new IOException("error when handling data directory, please check " + snapshotDir);
            }

            // 将数据文件复制到快照目录下
            FileUtils.copyDirectory(dataFile, snapshotFile);

            BitCaskOptions options = new BitCaskOptions();
            options.read_write = true;
            db = BitCask.open(dataFile, options);
        } catch (Exception e) {
            LOG.warn("writeSnapshot meet exception, msg={}", e.getMessage());
        }
    }

    @Override
    public void readSnapshot(String snapshotDir) {
        try {
            // 将快照目录复制到数据目录
            if (db != null) {
                db.close();
                db = null;
            }
            String dataDir = raftDataDir + File.separator + "leveldb_data";
            File dataFile = new File(dataDir);
            if (dataFile.exists()) {
                FileUtils.deleteDirectory(dataFile);
            }
            File snapshotFile = new File(snapshotDir);
            if (snapshotFile.exists()) {
                FileUtils.copyDirectory(snapshotFile, dataFile);
            }

            BitCaskOptions options = new BitCaskOptions();
            options.read_write = true;
            db = BitCask.open(dataFile, options);
        } catch (Exception e) {
            LOG.warn("meet exception, msg={}", e.getMessage());
        }
    }

    @Override
    public void apply(byte[] dataBytes) {
        try {
            ExampleProto.SetRequest request = ExampleProto.SetRequest.parseFrom(dataBytes);
            db.put(request.getKey(), request.getValue());
        } catch (Exception e) {
            LOG.warn("meet exception, msg={}", e.getMessage());
        }
    }

    @Override
    public byte[] get(byte[] dataBytes) {
        byte[] result = null;
        try {
            String value = db.getString(new String(dataBytes));
            if (value != null) {
                result = value.getBytes();
            }
        } catch (IOException e) {
            LOG.warn("read BitCaskDB error, msg={}", e.getMessage());
        }
        return result;
    }
}
