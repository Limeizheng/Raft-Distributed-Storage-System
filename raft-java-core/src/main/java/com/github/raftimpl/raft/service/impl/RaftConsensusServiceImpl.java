//实现Raft共识服务，处理节点间的共识相关通信
//通过 RaftNode 处理RPC请求，确保集群状态一致性和数据同步。
package com.github.raftimpl.raft.service.impl;

import com.github.raftimpl.raft.RaftNode;
import com.github.raftimpl.raft.proto.RaftProto;
import com.github.raftimpl.raft.service.RaftConsensusService;
import com.github.raftimpl.raft.util.ConfigurationUtils;
import com.github.raftimpl.raft.util.RaftFileUtils;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * Created by raftimpl on 2017/5/2.
 */
public class RaftConsensusServiceImpl implements RaftConsensusService {

    private static final Logger LOG = LoggerFactory.getLogger(RaftConsensusServiceImpl.class);//eturns a Logger instance associated with the specified class
    private static final JsonFormat PRINTER = new JsonFormat();

    private RaftNode raftNode;

    public RaftConsensusServiceImpl(RaftNode node) {
        this.raftNode = node;
    }

//    处理预投票请求
//    Pre-Vote Mechanism: An optional extension to Raft to prevent disruptive elections by having nodes check
//    whether they are likely to win an election before incrementing their term.
    @Override
    public RaftProto.VoteResponse preVote(RaftProto.VoteRequest request) {//input: The pre-vote request sent by a candidate node. output:The response indicating whether the vote is granted
        //Ensures thread safety by acquiring a lock on the raftNode before processing the request.
        // ReentrantLock -- the same thread can acquire the lock multiple times without causing a deadlock.
        // Any other operations that require the same lock (e.g., appending logs, processing other RPCs) will be paused until the lock is available.
        // The raftNode cannot perform other actions that require the lock during this time, ensuring that state changes are safely serialized.
        raftNode.getLock().lock();
        try {
            //RaftProto.VoteResponse.Builder: An inner static class used to build an instance of VoteResponse.
            //newBuilder() Method: Creates a new Builder instance for VoteResponse.Allows you to set fields using method chaining or individual setters. Ensures immutability of the message objects once built.
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
            responseBuilder.setGranted(false);
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            //3 conditions to grant the vote
            if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), request.getServerId())) {//Verifies that the requesting server (candidate) is part of the current cluster configuration
                return responseBuilder.build();//Return the response without granting the vote.
            }
            if (request.getTerm() < raftNode.getCurrentTerm()) {//Ensures that the candidate's term is not outdated
                return responseBuilder.build();
            }
            boolean isLogOk = request.getLastLogTerm() > raftNode.getLastLogTerm()//Determines whether the candidate's log is at least as up-to-date as the node's log.
                    || (request.getLastLogTerm() == raftNode.getLastLogTerm()
                    && request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
            if (!isLogOk) {
                return responseBuilder.build();
            } else {
                responseBuilder.setGranted(true);
                responseBuilder.setTerm(raftNode.getCurrentTerm());
            }
            LOG.info("preVote request from server {} " +
                            "in term {} (my term is {}), granted={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getGranted());
            return responseBuilder.build();//Constructs an immutable VoteResponse object from the builder.  Once built, the message cannot be modified, ensuring thread safety and consistency when sharing between threads or sending over the network.
        } finally {
            raftNode.getLock().unlock();//ensures the lock is released even if an exception occurs.
        }
    }

//    处理正式投票请求。该方法用于处理选举期间的投票请求。
    @Override
    public RaftProto.VoteResponse requestVote(RaftProto.VoteRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
            responseBuilder.setGranted(false);
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), request.getServerId())) {
                return responseBuilder.build();
            }
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            if (request.getTerm() > raftNode.getCurrentTerm()) {
                raftNode.stepDown(request.getTerm());// Even as a follower, the node needs to recognize the new term to stay consistent with the cluster.
            }
            boolean logIsOk = request.getLastLogTerm() > raftNode.getLastLogTerm()
                    || (request.getLastLogTerm() == raftNode.getLastLogTerm()
                    && request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
            if (raftNode.getVotedFor() == 0 && logIsOk) {//The node has not yet voted for any candidate in this term.
                //Ensures the node is a follower in the candidate's term. When a node grants a vote to a candidate, especially in a higher term,
                //it must ensure it is in the follower state within the candidate's term.
                // This is because only followers can vote for candidates, and stepping down prevents conflicts in leadership roles.
                //Calling stepDown is appropriate and necessary even if the node is already a follower because it ensures update term and reset state.
                raftNode.stepDown(request.getTerm());
                raftNode.setVotedFor(request.getServerId());
//              //Persisting metadata before responding to a vote request is crucial for maintaining the safety properties of Raft.
//              While it introduces some overhead, the amount of data is minimal, and the consistency guarantees it provides are essential.
                raftNode.getRaftLog().updateMetaData(raftNode.getCurrentTerm(), raftNode.getVotedFor(), null, null);//Updates persistent storage with the current term and votedFor to survive crashes.
                responseBuilder.setGranted(true);
                responseBuilder.setTerm(raftNode.getCurrentTerm());
            }
            LOG.info("RequestVote request from server {} " +
                            "in term {} (my term is {}), granted={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getGranted());
            return responseBuilder.build();
        } finally {
            raftNode.getLock().unlock();
        }
    }

    /**  处理追加日志条目的请求。该方法用于leader向follower复制日志条目，同时也用作心跳机制。
        The node (raftNode) can be in any of the three states—Follower, Candidate, or Leader—when it receives an AppendEntries RPC
        If the incoming AppendEntries request has a term greater than the node's current term, it indicates that a new leader with a higher term is active.
        If the incoming request has a term equal to the node's current term, it may indicate the presence of another leader or a network partition.
        If raftNode is a Leader:
        Receiving Higher Term: It must step down to prevent split-brain scenarios where multiple leaders exist in the cluster.
        Maintaining Safety: Stepping down ensures that only the node with the highest term acts as the leader, preserving Raft's safety guarantees.
        If raftNode is a Candidate:
        Term Update: Receiving an AppendEntries with a higher or equal term means another node is asserting leadership or there's an ongoing leadership contention.
        Transition to Follower: To resolve the contention and maintain cluster consistency, the node steps down to become a follower.
        If raftNode is Already a Follower:
        Term Update: Even as a follower, the node needs to update its term if it receives a higher term to stay consistent with the cluster's current state.
        No State Change: If already a follower, stepping down might simply update the term without changing the node's state.*/
    @Override
    public RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.AppendEntriesResponse.Builder responseBuilder
                    = RaftProto.AppendEntriesResponse.newBuilder();
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL); // Defaults to failure (RES_CODE_FAIL), which can be updated to success later if conditions are met.
            responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());

            //Term Verification
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }

            //Leader Step-Down and Identification
            raftNode.stepDown(request.getTerm());
            if (raftNode.getLeaderId() == 0) {
                raftNode.setLeaderId(request.getServerId());
                LOG.info("new leaderId={}, conf={}",
                        raftNode.getLeaderId(),
                        PRINTER.printToString(raftNode.getConfiguration()));//Converts a protobuf message into a formatted string.
            }
            if (raftNode.getLeaderId() != request.getServerId()) {
                LOG.warn("Another peer={} declares that it is the leader " +
                                "at term={} which was occupied by leader={}",
                        request.getServerId(), request.getTerm(), raftNode.getLeaderId());
                /**By setting its term to one higher than the incoming request's term, the node ensures that its term is
                now higher than the conflicting leader's term. The conflicting leader (with term request.getTerm()) can
                 no longer be recognized as the legitimate leader because Raft dictates that the leader with the highest
                 term is the authoritative leader. Other nodes in the cluster, upon recognizing the updated term, may
                 initiate a new election to elect a leader for the new term (request.getTerm() + 1).*/
                raftNode.stepDown(request.getTerm() + 1);
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);
                /**The response includes the updated term (request.getTerm() + 1), informing the leader that the follower's
                 * term has advanced, potentially invalidating the leader's authority.*/
                responseBuilder.setTerm(request.getTerm() + 1);
                return responseBuilder.build();
            }

            //Log Consistency Checks
            if (request.getPrevLogIndex() > raftNode.getRaftLog().getLastLogIndex()) {//Ensures that the leader's log does not have a gap relative to the follower's log.
                LOG.info("Rejecting AppendEntries RPC would leave gap, " +
                        "request prevLogIndex={}, my lastLogIndex={}",
                        request.getPrevLogIndex(), raftNode.getRaftLog().getLastLogIndex());
                return responseBuilder.build();
            }
            if (request.getPrevLogIndex() >= raftNode.getRaftLog().getFirstLogIndex()
                    && raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex())
                    != request.getPrevLogTerm()) {
                LOG.info("Rejecting AppendEntries RPC: terms don't agree, " +
                        "request prevLogTerm={} in prevLogIndex={}, my is {}",
                        request.getPrevLogTerm(), request.getPrevLogIndex(),
                        raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex()));
                //Ensures that prevLogIndex is a positive number.If prevLogIndex is not greater than 0, an
                //IllegalArgumentException is thrown, preventing further execution with invalid data.
                Validate.isTrue(request.getPrevLogIndex() > 0);
                //indicating that the follower's log is consistent up to prevLogIndex - 1.This information guides the
                //leader to adjust its nextIndex for the follower,  ensuring that it resends the correct log entries starting from the last consistent index.
                responseBuilder.setLastLogIndex(request.getPrevLogIndex() - 1);
                return responseBuilder.build();
            }

//            Processes heartbeat messages from the leader, which are AppendEntries RPCs with no log entries.
            if (request.getEntriesCount() == 0) {
                LOG.debug("heartbeat request from peer={} at term={}, my term={}",
                        request.getServerId(), request.getTerm(), raftNode.getCurrentTerm());
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
                responseBuilder.setTerm(raftNode.getCurrentTerm());
                responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());
                advanceCommitIndex(request);//Updates the follower's commit index based on the leader's information, ensuring that any newly committed entries are applied to the state machine.
                return responseBuilder.build();
            }

            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
            List<RaftProto.LogEntry> entries = new ArrayList<>();
            long index = request.getPrevLogIndex();
            for (RaftProto.LogEntry entry : request.getEntriesList()) {
                index++;
                if (index < raftNode.getRaftLog().getFirstLogIndex()) {
                    continue;
                }
                if (raftNode.getRaftLog().getLastLogIndex() >= index) {
                    if (raftNode.getRaftLog().getEntryTerm(index) == entry.getTerm()) {// Check for Existing Entries at Current Index
                        continue;
                    }
                    // truncate segment log from index
                    long lastIndexKept = index - 1;
                    raftNode.getRaftLog().truncateSuffix(lastIndexKept);//Raft logs are typically stored persistently on disk to survive node crashes and restarts.LogEntry is in-memory
                }
                entries.add(entry);
            }
            raftNode.getRaftLog().append(entries);
//            raftNode.getRaftLog().updateMetaData(raftNode.getCurrentTerm(),
//                    null, raftNode.getRaftLog().getFirstLogIndex());
            responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());

            advanceCommitIndex(request);
            LOG.info("AppendEntries request from server {} " +
                            "in term {} (my term is {}), entryCount={} resCode={}",
                    request.getServerId(), request.getTerm(), raftNode.getCurrentTerm(),
                    request.getEntriesCount(), responseBuilder.getResCode());
            return responseBuilder.build();
        } finally {
            raftNode.getLock().unlock();
        }
    }

//    处理安装快照的请求。当日志条目过多时，leader会向follower发送快照数据，以减少数据同步的负担。
    @Override
    public RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request) {
        RaftProto.InstallSnapshotResponse.Builder responseBuilder
                = RaftProto.InstallSnapshotResponse.newBuilder();
        responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);

        raftNode.getLock().lock();
        try {
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            raftNode.stepDown(request.getTerm());
            if (raftNode.getLeaderId() == 0) {
                raftNode.setLeaderId(request.getServerId());
                LOG.info("new leaderId={}, conf={}",
                        raftNode.getLeaderId(),
                        PRINTER.printToString(raftNode.getConfiguration()));
            }
        } finally {
            raftNode.getLock().unlock();
        }

        if (raftNode.getSnapshot().getIsTakeSnapshot().get()) {
            LOG.warn("alreay in take snapshot, do not handle install snapshot request now");
            return responseBuilder.build();
        }

        raftNode.getSnapshot().getIsInstallSnapshot().set(true);
        RandomAccessFile randomAccessFile = null;
        raftNode.getSnapshot().getLock().lock();
        try {
            // write snapshot data to local
            String tmpSnapshotDir = raftNode.getSnapshot().getSnapshotDir() + ".tmp";
            File file = new File(tmpSnapshotDir);
            if (request.getIsFirst()) {
                if (file.exists()) {
                    file.delete();
                }
                file.mkdir();
                LOG.info("begin accept install snapshot request from serverId={}", request.getServerId());
                raftNode.getSnapshot().updateMetaData(tmpSnapshotDir,
                        request.getSnapshotMetaData().getLastIncludedIndex(),
                        request.getSnapshotMetaData().getLastIncludedTerm(),
                        request.getSnapshotMetaData().getConfiguration());
            }
            // write to file
            String currentDataDirName = tmpSnapshotDir + File.separator + "data";
            File currentDataDir = new File(currentDataDirName);
            if (!currentDataDir.exists()) {
                currentDataDir.mkdirs();
            }

            String currentDataFileName = currentDataDirName + File.separator + request.getFileName();
            File currentDataFile = new File(currentDataFileName);
            // 文件名可能是个相对路径，比如topic/0/message.txt
            if (!currentDataFile.getParentFile().exists()) {
                currentDataFile.getParentFile().mkdirs();
            }
            if (!currentDataFile.exists()) {
                currentDataFile.createNewFile();
            }
            randomAccessFile = RaftFileUtils.openFile(
                    tmpSnapshotDir + File.separator + "data",
                    request.getFileName(), "rw");
            randomAccessFile.seek(request.getOffset());
            randomAccessFile.write(request.getData().toByteArray());
            // move tmp dir to snapshot dir if this is the last package
            if (request.getIsLast()) {
                File snapshotDirFile = new File(raftNode.getSnapshot().getSnapshotDir());
                if (snapshotDirFile.exists()) {
                    FileUtils.deleteDirectory(snapshotDirFile);
                }
                FileUtils.moveDirectory(new File(tmpSnapshotDir), snapshotDirFile);
            }
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
            LOG.info("install snapshot request from server {} " +
                            "in term {} (my term is {}), resCode={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getResCode());
        } catch (IOException ex) {
            LOG.warn("when handle installSnapshot request, meet exception:", ex);
        } finally {
            RaftFileUtils.closeFile(randomAccessFile);
            raftNode.getSnapshot().getLock().unlock();
        }

        if (request.getIsLast() && responseBuilder.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {
            // apply state machine
            // TODO: make this async
            String snapshotDataDir = raftNode.getSnapshot().getSnapshotDir() + File.separator + "data";
            raftNode.getStateMachine().readSnapshot(snapshotDataDir);
            long lastSnapshotIndex;
            // 重新加载snapshot
            raftNode.getSnapshot().getLock().lock();
            try {
                raftNode.getSnapshot().reload();
                lastSnapshotIndex = raftNode.getSnapshot().getMetaData().getLastIncludedIndex();
            } finally {
                raftNode.getSnapshot().getLock().unlock();
            }

            // discard old log entries
            raftNode.getLock().lock();
            try {
                raftNode.getRaftLog().truncatePrefix(lastSnapshotIndex + 1);
            } finally {
                raftNode.getLock().unlock();
            }
            LOG.info("end accept install snapshot request from serverId={}", request.getServerId());
        }

        if (request.getIsLast()) {
            raftNode.getSnapshot().getIsInstallSnapshot().set(false);
        }

        return responseBuilder.build();
    }

    // in lock, for follower
//    在追加日志条目后，更新提交索引（commitIndex）。
//    提交索引更新。在接收到新的日志条目后，**follower**会更新其提交索引。
//    在日志条目或快照被确认提交后，状态机将应用这些更改。在 advanceCommitIndex 中，如果 commitIndex 大于 lastAppliedIndex，
//    则会调用状态机的 apply 方法，应用日志条目到状态机。
//    Synchronize Commit Index: Keeps the follower's commit index up-to-date with the leader's, facilitating consistent application of committed entries to the state machine.
    private void advanceCommitIndex(RaftProto.AppendEntriesRequest request) {
        long newCommitIndex = Math.min(request.getCommitIndex(),
                request.getPrevLogIndex() + request.getEntriesCount());
        raftNode.setCommitIndex(newCommitIndex);
        raftNode.getRaftLog().updateMetaData(null,null, null, newCommitIndex);
        if (raftNode.getLastAppliedIndex() < raftNode.getCommitIndex()) {
            // apply state machine
            for (long index = raftNode.getLastAppliedIndex() + 1;
                 index <= raftNode.getCommitIndex(); index++) {
                RaftProto.LogEntry entry = raftNode.getRaftLog().getEntry(index);
                if (entry != null) {
                    if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_DATA) {
                        raftNode.getStateMachine().apply(entry.getData().toByteArray());//Applying follower's Committed Entries to the State Machine
                    } else if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_CONFIGURATION) {
                        raftNode.applyConfiguration(entry);
                    }
                }
                raftNode.setLastAppliedIndex(index);
            }
        }
    }

}
