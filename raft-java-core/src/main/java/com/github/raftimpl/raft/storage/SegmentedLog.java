package com.github.raftimpl.raft.storage;

import com.github.raftimpl.raft.RaftOptions;
import com.github.raftimpl.raft.util.RaftFileUtils;
import com.github.raftimpl.raft.proto.RaftProto;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

//管理Raft日志的分段存储
public class SegmentedLog {

    private static Logger LOG = LoggerFactory.getLogger(SegmentedLog.class);

    private String logDir;
    private String logDataDir;
    private int maxSegmentFileSize;
    private RaftProto.LogMetaData metaData;
    private TreeMap<Long, Segment> startLogIndexSegmentMap = new TreeMap<>();//将日志的开始索引映射到对应的Segment, treeMap是有序的
    private volatile long totalSize;// segment log占用的内存大小，用于判断是否需要做snapshot

//    构造主要function初始化日志目录和最大段文件大小
//    /path/to/raft/           <-- raftDataDir
//            ├── log/               <-- logDir: The root log directory.
//            │   ├── data/             <-- logDataDir (contains log segment files, e.g., term, votedFor)
//│   └── metadata          <-- Metadata file (log metadata)
//    each segment is stored in its own file within the logDataDir directory.

    public SegmentedLog(String raftDataDir, int maxSegmentFileSize) {
        this.logDir = raftDataDir + File.separator + "log";
        this.logDataDir = logDir + File.separator + "data";
        this.maxSegmentFileSize = maxSegmentFileSize;
        File file = new File(logDataDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        readSegments();
        for (Segment segment : startLogIndexSegmentMap.values()) {
            this.loadSegmentData(segment);
        }

        metaData = this.readMetaData();
        if (metaData == null) {
            if (startLogIndexSegmentMap.size() > 0) {
                LOG.error("No readable metadata file but found segments in {}", logDir);
                throw new RuntimeException("No readable metadata file but found segments");
            }
            metaData = RaftProto.LogMetaData.newBuilder().setFirstLogIndex(1).build();
        }
    }

    public RaftProto.LogEntry getEntry(long index) {//根据索引读取日志条目
        long firstLogIndex = getFirstLogIndex();
        long lastLogIndex = getLastLogIndex();
        if (index == 0 || index < firstLogIndex || index > lastLogIndex) {
            LOG.debug("index out of range, index={}, firstLogIndex={}, lastLogIndex={}",
                    index, firstLogIndex, lastLogIndex);
            return null;
        }
        if (startLogIndexSegmentMap.size() == 0) {
            return null;
        }
        Segment segment = startLogIndexSegmentMap.floorEntry(index).getValue();
        return segment.getEntry(index);
    }

    public long getEntryTerm(long index) {//获取指定日志条目的任期
        RaftProto.LogEntry entry = getEntry(index);
        if (entry == null) {
            return 0;
        }
        return entry.getTerm();
    }

    public long getFirstLogIndex() {
        return metaData.getFirstLogIndex();
    }

    public long getLastLogIndex() {
        // 有两种情况segment为空
        // 1、第一次初始化，firstLogIndex = 1，lastLogIndex = 0
        // 2、snapshot刚完成，日志正好被清理掉，firstLogIndex = snapshotIndex + 1， lastLogIndex = snapshotIndex
        if (startLogIndexSegmentMap.size() == 0) {
            return getFirstLogIndex() - 1;
        }
        Segment lastSegment = startLogIndexSegmentMap.lastEntry().getValue();
        return lastSegment.getEndIndex();
    }

    public long append(List<RaftProto.LogEntry> entries) {//追加日志条目，如果当前段无法写入或文件大小超过最大值，则创建新的段
        long newLastLogIndex = this.getLastLogIndex();
        for (RaftProto.LogEntry entry : entries) {
            newLastLogIndex++;
            int entrySize = entry.getSerializedSize();
            int segmentSize = startLogIndexSegmentMap.size();
            boolean isNeedNewSegmentFile = false;
            try {
                if (segmentSize == 0) {
                    isNeedNewSegmentFile = true;
                } else {
                    Segment segment = startLogIndexSegmentMap.lastEntry().getValue();
                    if (!segment.isCanWrite()) {
                        isNeedNewSegmentFile = true;
                    } else if (segment.getFileSize() + entrySize >= maxSegmentFileSize) {
                        isNeedNewSegmentFile = true;
                        // 最后一个segment的文件close并改名
                        segment.getRandomAccessFile().close();
                        segment.setCanWrite(false);
                        String newFileName = String.format("%020d-%020d",
                                segment.getStartIndex(), segment.getEndIndex());
                        String newFullFileName = logDataDir + File.separator + newFileName;
                        File newFile = new File(newFullFileName);
                        String oldFullFileName = logDataDir + File.separator + segment.getFileName();
                        File oldFile = new File(oldFullFileName);
                        FileUtils.moveFile(oldFile, newFile);
                        segment.setFileName(newFileName);
                        segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, newFileName, "r"));
                    }
                }
                Segment newSegment;
                // 新建segment文件
                if (isNeedNewSegmentFile) {
                    // open new segment file
                    String newSegmentFileName = String.format("open-%d", newLastLogIndex);
                    String newFullFileName = logDataDir + File.separator + newSegmentFileName;
                    File newSegmentFile = new File(newFullFileName);
                    if (!newSegmentFile.exists()) {
                        newSegmentFile.createNewFile();
                    }
                    Segment segment = new Segment();
                    segment.setCanWrite(true);
                    segment.setStartIndex(newLastLogIndex);
                    segment.setEndIndex(0);
                    segment.setFileName(newSegmentFileName);
                    segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, newSegmentFileName, "rw"));
                    newSegment = segment;
                } else {
                    newSegment = startLogIndexSegmentMap.lastEntry().getValue();
                }
                // 写proto到segment中
                if (entry.getIndex() == 0) {
                    entry = RaftProto.LogEntry.newBuilder(entry)
                            .setIndex(newLastLogIndex).build();
                }
                newSegment.setEndIndex(entry.getIndex());
                newSegment.getEntries().add(new Segment.Record(
                        newSegment.getRandomAccessFile().getFilePointer(), entry));
                RaftFileUtils.writeProtoToFile(newSegment.getRandomAccessFile(), entry);// Persists the log entry to disk by writing it into the segment file.
                newSegment.setFileSize(newSegment.getRandomAccessFile().length());
                if (!startLogIndexSegmentMap.containsKey(newSegment.getStartIndex())) {
                    startLogIndexSegmentMap.put(newSegment.getStartIndex(), newSegment);
                }
                totalSize += entrySize;
            }  catch (IOException ex) {
                throw new RuntimeException("append raft log exception, msg=" + ex.getMessage());
            }
        }
        return newLastLogIndex;
    }
    //截断日志的前缀，用于处理已应用的日志--Purpose: Removes entries from the beginning (prefix) of the log up to
    // a specified index. Use Case: Handles logs that have been applied to the state machine and possibly snapshotted,
    // allowing for log compaction and freeing up storage
    public void truncatePrefix(long newFirstIndex) {
        if (newFirstIndex <= getFirstLogIndex()) {
            return;
        }
        long oldFirstIndex = getFirstLogIndex();
        while (!startLogIndexSegmentMap.isEmpty()) {
            Segment segment = startLogIndexSegmentMap.firstEntry().getValue();
            if (segment.isCanWrite()) {
                break;
            }
            if (newFirstIndex > segment.getEndIndex()) {
                File oldFile = new File(logDataDir + File.separator + segment.getFileName());
                try {
                    RaftFileUtils.closeFile(segment.getRandomAccessFile());
                    FileUtils.forceDelete(oldFile);
                    totalSize -= segment.getFileSize();
                    startLogIndexSegmentMap.remove(segment.getStartIndex());
                } catch (Exception ex2) {
                    LOG.warn("delete file exception:", ex2);
                }
            } else {
                break;
            }
        }
        long newActualFirstIndex;
        if (startLogIndexSegmentMap.size() == 0) {
            newActualFirstIndex = newFirstIndex;
        } else {
            newActualFirstIndex = startLogIndexSegmentMap.firstKey();
        }
        updateMetaData(null, null, newActualFirstIndex, null);
        LOG.info("Truncating log from old first index {} to new first index {}",
                oldFirstIndex, newActualFirstIndex);
    }
//    Purpose: Removes entries from the end (suffix) of the log starting after a specified index.
//    Use Case: Resolves log inconsistencies or conflicts when a follower's log contains entries that do not match the leader's log.
    public void truncateSuffix(long newEndIndex) {//截断日志的后缀，处理日志冲突情况
        if (newEndIndex >= getLastLogIndex()) {
            return;
        }
        LOG.info("Truncating log from old end index {} to new end index {}",
                getLastLogIndex(), newEndIndex);
        while (!startLogIndexSegmentMap.isEmpty()) {
            Segment segment = startLogIndexSegmentMap.lastEntry().getValue();
            try {
                if (newEndIndex == segment.getEndIndex()) {
                    break;
                } else if (newEndIndex < segment.getStartIndex()) {
                    totalSize -= segment.getFileSize();
                    // delete file
                    segment.getRandomAccessFile().close();
                    String fullFileName = logDataDir + File.separator + segment.getFileName();
                    FileUtils.forceDelete(new File(fullFileName));
                    startLogIndexSegmentMap.remove(segment.getStartIndex());
                } else if (newEndIndex < segment.getEndIndex()) {
                    int i = (int) (newEndIndex + 1 - segment.getStartIndex());
                    segment.setEndIndex(newEndIndex);
                    long newFileSize = segment.getEntries().get(i).offset;
                    totalSize -= (segment.getFileSize() - newFileSize);
                    segment.setFileSize(newFileSize);
                    segment.getEntries().removeAll(
                            segment.getEntries().subList(i, segment.getEntries().size()));
                    FileChannel fileChannel = segment.getRandomAccessFile().getChannel();
                    fileChannel.truncate(segment.getFileSize());
                    fileChannel.close();
                    segment.getRandomAccessFile().close();
                    String oldFullFileName = logDataDir + File.separator + segment.getFileName();
                    String newFileName = String.format("%020d-%020d",
                            segment.getStartIndex(), segment.getEndIndex());
                    segment.setFileName(newFileName);
                    String newFullFileName = logDataDir + File.separator + segment.getFileName();
                    new File(oldFullFileName).renameTo(new File(newFullFileName));
                    segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, segment.getFileName(), "rw"));
                }
            } catch (IOException ex) {
                LOG.warn("io exception, msg={}", ex.getMessage());
            }
        }
    }

    public void loadSegmentData(Segment segment) {//加载特定段的数据
        try {
            RandomAccessFile randomAccessFile = segment.getRandomAccessFile();
            long totalLength = segment.getFileSize();
            long offset = 0;
            while (offset < totalLength) {
                RaftProto.LogEntry entry = RaftFileUtils.readProtoFromFile(
                        randomAccessFile, RaftProto.LogEntry.class);
                if (entry == null) {
                    throw new RuntimeException("read segment log failed");
                }
                Segment.Record record = new Segment.Record(offset, entry);
                segment.getEntries().add(record);
                offset = randomAccessFile.getFilePointer();
            }
            totalSize += totalLength;
        } catch (Exception ex) {
            LOG.error("read segment meet exception, msg={}", ex.getMessage());
            throw new RuntimeException("file not found");
        }

        int entrySize = segment.getEntries().size();
        if (entrySize > 0) {
            segment.setStartIndex(segment.getEntries().get(0).entry.getIndex());
            segment.setEndIndex(segment.getEntries().get(entrySize - 1).entry.getIndex());
        }
    }

    public void readSegments() {//读取日志数据目录中的所有日志段
        try {
            List<String> fileNames = RaftFileUtils.getSortedFilesInDirectory(logDataDir, logDataDir);
            for (String fileName : fileNames) {
                String[] splitArray = fileName.split("-");
                if (splitArray.length != 2) {
                    LOG.warn("segment filename[{}] is not valid", fileName);
                    continue;
                }
                Segment segment = new Segment();
                segment.setFileName(fileName);
                if (splitArray[0].equals("open")) {
                    segment.setCanWrite(true);
                    segment.setStartIndex(Long.valueOf(splitArray[1]));
                    segment.setEndIndex(0);
                } else {
                    try {
                        segment.setCanWrite(false);
                        segment.setStartIndex(Long.parseLong(splitArray[0]));
                        segment.setEndIndex(Long.parseLong(splitArray[1]));
                    } catch (NumberFormatException ex) {
                        LOG.warn("segment filename[{}] is not valid", fileName);
                        continue;
                    }
                }
                segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, fileName, "rw"));
                segment.setFileSize(segment.getRandomAccessFile().length());
                startLogIndexSegmentMap.put(segment.getStartIndex(), segment);
            }
        } catch(IOException ioException){
            LOG.warn("readSegments exception:", ioException);
            throw new RuntimeException("open segment file error");
        }
    }

//    On Disk: The metadata is persisted on disk in the specified file.
//    In Memory: When the method readMetaData() is called, it reads the metadata from the file into memory as a RaftProto.LogMetaData object.
    public RaftProto.LogMetaData readMetaData() {
        String fileName = logDir + File.separator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            RaftProto.LogMetaData metadata = RaftFileUtils.readProtoFromFile(
                    randomAccessFile, RaftProto.LogMetaData.class);
            return metadata;
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
            return null;
        }
    }

    /**
     * 更新raft log meta data，
     * 包括commitIndex， fix bug: https://github.com/raftimpl/raft-java/issues/19
     * @param currentTerm
     * @param votedFor
     * @param firstLogIndex
     * @param commitIndex
     */
    public void updateMetaData(Long currentTerm, Integer votedFor, Long firstLogIndex, Long commitIndex) {
        RaftProto.LogMetaData.Builder builder = RaftProto.LogMetaData.newBuilder(this.metaData); //metadata is in-memory
        if (currentTerm != null) {
            builder.setCurrentTerm(currentTerm);
        }
        if (votedFor != null) {
            builder.setVotedFor(votedFor);
        }
        if (firstLogIndex != null) {
            builder.setFirstLogIndex(firstLogIndex);
        }
        if (commitIndex != null) {
            builder.setCommitIndex(commitIndex);
        }
        this.metaData = builder.build();

        String fileName = logDir + File.separator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {//read-write mode
            RaftFileUtils.writeProtoToFile(randomAccessFile, metaData);// Write the updated metadata to disk
            LOG.info("new segment meta info, currentTerm={}, votedFor={}, firstLogIndex={}",
                    metaData.getCurrentTerm(), metaData.getVotedFor(), metaData.getFirstLogIndex());
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
        }
    }

    public RaftProto.LogMetaData getMetaData() {
        return metaData;
    }

    public long getTotalSize() {
        return totalSize;
    }

}
