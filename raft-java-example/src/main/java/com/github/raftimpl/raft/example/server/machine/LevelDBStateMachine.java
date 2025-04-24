package com.github.raftimpl.raft.example.server.machine;

import btree4j.BTreeException;
import com.github.raftimpl.raft.StateMachine;
import com.github.raftimpl.raft.example.server.service.ExampleProto;
import org.apache.commons.io.FileUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class LevelDBStateMachine implements StateMachine {
    private static final Logger LOG = LoggerFactory.getLogger(LevelDBStateMachine.class);
    private DB db;
    private final String raftDataDir;

    public LevelDBStateMachine(String raftDataDir) {
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

            Options options = new Options();
            db = Iq80DBFactory.factory.open(dataFile, options);
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

            Options options = new Options();
            db = Iq80DBFactory.factory.open(dataFile, options);
        } catch (Exception e) {
            LOG.warn("meet exception, msg={}", e.getMessage());
        }
    }

    @Override
    public void apply(byte[] dataBytes) {
        try {
            if (db == null) {
                throw new BTreeException("database is closed, please wait for reopen");
            }
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
            if (db == null) {
                throw new DBException("database is closed, please wait for reopen");
            }
            result = db.get(dataBytes);
        } catch (Exception e) {
            LOG.warn("read leveldb exception, msg={}", e.getMessage());
        }
        return result;
    }
}
