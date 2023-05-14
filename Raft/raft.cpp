#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <time.h>
#include <sys/time.h>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include "./buttonrpc-master/buttonrpc.hpp"
using namespace std;

#define COMMON_PORT 1234
#define HEART_BEAT_PERIOD 100000

//需要结合LAB3实现应用层dataBase和Raft交互用的，通过getCmd()转化为applyMsg的command
//实际上这只是LAB2的raft.hpp，在LAB3中改了很多，LAB4又改了不少，所以每个LAB都引了单独的raft.hpp
class Operation {
public:
    string getCmd();
    string op;
    string key;
    string value;
    int clientId;
    int requestId;
};

string Operation::getCmd() {
    string cmd = op + " " + key + " " + value;
    return cmd;
}

//通过传入raft.start()得到的返回值，封装成类
class StartRet {
public:
    StartRet() : cmdIndex(-1), curTerm(-1), isLeader(false) {}
    int cmdIndex;
    int curTerm;
    bool isLeader;
};

//同应用层交互的需要提交到应用层并apply的封装成applyMsg的日志信息
class ApplyMsg {
    bool CommandValid;
	string command;
    int CommandIndex;
};

// 封装了各个server暴露出来的RPC端口号以及自己对应的RaftId
// 每个server有两个RPC端口号(为了减轻负担，一个选举，一个日志同步)
struct PeersInfo {
public:
    pair<int, int> port;
    int peerId;
};

//日志
class LogEntry {
public:
    LogEntry(string cmd = "", int term = -1): command(cmd), term(term){}
    string command;
    int term;
};

//持久化类，LAB2中需要持久化的内容就这3个，后续会修改
struct Persister {
public:
    int currentTerm;
    int votedFor;
    vector<LogEntry> logs;
};

struct RequestVoteArgs {
public:
    int term;           // candidate’s term
    int candidateId; 
    int lastLogIndex;   //index of candidate’s last log entry
    int lastLogTerm;    //term of candidate’s last log entry
};

struct RequestVoteReply {
public:
    int term;          // currentTerm, for candidate to update itself
    bool voteGranted;  // true means candidate received vote
};

class AppendEntriesArgs {
public:
    int term;         // leader’s term
    int leaderId;     // so follower can redirect clients
    int prevLogIndex; // index of log entry immediately preceding new ones
    int prevLogTerm;  // term of prevLogIndex entry
    int leaderCommit; // leader’s commitIndex
    string sendLogs;  // 发送的日志条目（对于心跳为空；为了提高效率，可能会发送多个）
    friend Serializer& operator >> (Serializer& in, AppendEntriesArgs& d) {
        in >> d.term >> d.leaderId >> d.prevLogIndex >> d.prevLogTerm >> d.prevLogTerm >> d.leaderCommit >> d.sendLogs;
        return in;
    }
    friend Serializer& operator << (Serializer& out, AppendEntriesArgs d) {
		out << d.term << d.leaderId << d.prevLogIndex << d.prevLogTerm << d.leaderCommit << d.sendLogs;
		return out;
	}
};

struct AppendEntriesReply {
public:
    int term;           // currentTerm, for leader to update itself
    bool success;       //true if follower contained entry matching prevLogIndex and prevLogTerm
    int conflict_term;        //用于冲突时日志快速匹配
    int conflict_index;       //用于冲突时日志快速匹配
};

//------------------------------核心类-------------------------------
class Raft {
public:
    enum RAFT_STATE {LEADER = 0, CANDIDATE, FOLLOWER};//用枚举定义的raft三种状态
    void Make(vector<PeersInfo> peers, int id);        //raft初始化
    RequestVoteReply requestVote(RequestVoteArgs args);// Invoked by candidates to gather votes; RPC调用
    AppendEntriesReply appendEntries(AppendEntriesArgs args);// Invoked by leader to replicate log entries; also used as heartbeat; RPC调用
    pair<int, bool> getState();//在LAB3中会用到，提前留出来的接口判断是否leader
    StartRet start(Operation op);//向raft传日志的函数，只有leader响应并立即返回，应用层用到
    void kill();//设定raft状态为dead，LAB3B快照测试时会用到
private:
    void listenForVote();       //用于监听voteRPC的server线程
    void listenForAppend();     //用于监听appendRPC的server线程
    void applyLogLoop();        //持续向上层应用日志的守护线程
    void electionLoop();        //持续处理选举的守护线程
    void processEntriesLoop();  //持续处理日志同步的守护线程
    void callRequestVote();     //发voteRPC的线程
    void sendAppendEntries();   //发appendRPC的线程

    //判断是否最新日志(两个准则)，requestVote时会用到
    bool checkLogUptodate(int lastLogTerm, int lastLogIndex);  
    //传入某个特定时间，计算该时间到当下的持续时间
    int getMyduration(timeval lastTime);
    //用的RPC不支持传容器，所以封装成string，这个是解封装恢复函数
    vector<LogEntry> getCmdAndTerm(string text);
    //重新设定BroadcastTime，成为leader发心跳的时候需要重置
    void setBroadcastTime();

    void saveRaftState();       //保存持久化状态
    void serialize();           //序列化
    void readRaftState();       //读取持久化状态
    bool deserialize();         //反序列化

private:
    mutex m_lock;
    condition_variable m_cond;
    vector<PeersInfo> m_peers;
    int m_peerId;  //m_peers中的索引，标记自己是哪个server
    bool m_dead;

    Persister m_persister;

    RAFT_STATE m_state;
    struct timeval m_lastWakeTime;
    struct timeval m_lastBroadcastTime;

    int cur_peerId; //表示哪些peer server是已经发过的(递增,一轮投票后重置为0)
    int recvVotes;
    int finishedVotes;

    //需要持久化的数据 (在所有server上)
    //m_currentTerm 和 m_votedFor 都是用来确保每个任期只有最多一个Leader
    int m_currentTerm;      //latest term server has seen 初始化为0，单调递增
    int m_votedFor;         //记录为哪个server投过票
    vector<LogEntry> m_logs;

    //易变的数据 (在所有server上)
    int m_commitIndex;//已知要提交的最高日志项的索引（初始化为0，单调递增）
    int m_lastApplied;//应用于状态机的最高日志项的索引（初始化为0，单调递增）

    //易变的数据 (在leader sever上)
    vector<int> m_nextIndex;//对于每个server，发送到该server的下一个日志条目的索引（初始化为leader last log index+1）
    vector<int> m_matchIndex;//对于每个server，已知要在该server上复制的最高日志条目的索引（初始化为0，单调递增）
};
//------------------------------核心类-------------------------------

void Raft::Make(vector<PeersInfo> peers, int id) {
    m_peers = peers;
    m_peerId = id;
    m_dead = false;

    m_state = FOLLOWER;
    gettimeofday(&m_lastWakeTime, NULL);

    cur_peerId = 0;
    recvVotes = 0;
    finishedVotes = 0;

    m_currentTerm = 0;
    m_votedFor = -1;

    m_commitIndex = 0;
    m_lastApplied = 0;

    m_nextIndex.resize(peers.size(), 1);
    m_matchIndex.resize(peers.size(), 0);

    thread listen_tid1(&Raft::listenForVote, this);
    listen_tid1.detach();
    thread listen_tid2(&Raft::listenForAppend, this);
    listen_tid2.detach();
    thread listen_tid3(&Raft::applyLogLoop, this);
    listen_tid3.detach();
}

//用于监听requestVote RPC调用的server线程
void Raft::listenForVote() {
    buttonrpc server;
    server.as_server(m_peers[m_peerId].port.first);
    server.bind("requestVote", &Raft::requestVote, this);

    //创建处理选举的线程
    thread wait_tid(&Raft::electionLoop, this);
    wait_tid.detach();

    server.run();
    printf("exit!\n");
}   

//用于监听appendRPC的server线程
void Raft::listenForAppend() {
    buttonrpc server;
    server.as_server(m_peers[m_peerId].port.second);
    server.bind("appendEntries", &Raft::appendEntries, this);

    //创建处理日志同步以及心跳的线程
    thread heart_tid(&Raft::processEntriesLoop, this);
    heart_tid.detach();

    server.run();
    printf("exit!\n");
}   

void Raft::applyLogLoop() {
    while (!m_dead) {
        usleep(10000);
        m_lock.lock();
        for (int i = m_lastApplied; i < m_commitIndex; i++) {
            /**
             * @brief 封装好信息发回给客户端, LAB3中会用
             *     ApplyMsg msg;
             * 
             */
        }
        m_lastApplied = m_commitIndex;
        m_lock.unlock();
    }
}

//持续处理选举的守护线程 
void Raft::electionLoop() {
    bool resetFlag = false;
    while (!m_dead) {
        //为选举定时器随机选择超时时间 超时时间在200-400ms
        int timeOut = rand() % 200000 + 200000; 
        while (1) {
            //sleep()以秒为单位、usleep()以微秒为单位
            usleep(1000);
            unique_lock<mutex> lock(m_lock);

            int during_time = getMyduration(m_lastWakeTime);
            if (m_state == FOLLOWER && during_time > timeOut) {
                m_state = CANDIDATE;
            }
            if (m_state == CANDIDATE && during_time > timeOut) {
                printf("%d attemp election at term %d, timeOut is %d\n", m_peerId, m_currentTerm, timeOut);
                gettimeofday(&m_lastWakeTime, NULL);
                resetFlag = true;
                m_currentTerm++;
                m_votedFor = m_peerId;//成为候选人后，把票投给自己
                saveRaftState();//？？？这里为什么要save一下

                this->recvVotes = 1;
                this->finishedVotes = 1;
                this->cur_peerId = 0;

                thread tid[m_peers.size() - 1];
                int i = 0;
                for (auto server : m_peers) {
                    if (server.peerId == m_peerId) continue;
                    tid[i] = thread(&Raft::callRequestVote, this);
                    tid[i].detach();
                    i++;
                }
                /*while (recvVotes <= m_peers.size() / 2 && finishedVotes != m_peers.size()) {
                    m_cond.wait(lock);
                }*/
                m_cond.wait(lock, [this]{
                    if (recvVotes <= m_peers.size() / 2 && finishedVotes != m_peers.size()) return false;
                    return true;
                });
                if (m_state != CANDIDATE) {
                    lock.unlock();
                    continue;
                }
                if (recvVotes > m_peers.size() / 2) {
                    m_state = LEADER;
                    //？？？
                    for (int i = 0; i < m_peers.size(); i++) {
                        m_nextIndex[i] = m_logs.size() + 1;
                        m_matchIndex[i] = 0;
                    }
                    printf("%d become new leader at term %d\n", m_peerId, m_currentTerm);
                    setBroadcastTime();//？？？？？
                }
            }
            lock.unlock();
            if (resetFlag) {
                resetFlag = false;
                break;
            }
        }
    }
}

void Raft::processEntriesLoop() {
    while (!m_dead) {
        usleep(1000);
        m_lock.lock();
        if (m_state != LEADER) {//只有leader能发送AppendEntries
            m_lock.unlock();
            continue;
        }
        int during_time = getMyduration(m_lastBroadcastTime);
        if (during_time < HEART_BEAT_PERIOD) {
            m_lock.unlock();
            continue;
        }
        gettimeofday(&m_lastBroadcastTime, NULL);//更新m_lastBroadcastTime
        m_lock.unlock();

        thread tid[m_peers.size() - 1];
        int i = 0;
        for (auto server : m_peers) {
            if (server.peerId == m_peerId) continue;
            tid[i] = thread(&Raft::sendAppendEntries, this);
            tid[i].detach();
            i++;
        }
    }
}

//发起requestVote调用的线程
void Raft::callRequestVote() {
    buttonrpc client;
    m_lock.lock();
    //args填的信息是发起选举的候选人的信息
    RequestVoteArgs args;
    args.candidateId = m_peerId;
    args.term = m_currentTerm;
    args.lastLogIndex = m_logs.size();
    args.lastLogTerm = m_logs.size() != 0 ? m_logs.back().term : 0;

    // 跳过发起选举的候选人这个server，直接递增
    if (cur_peerId == m_peerId) {
        cur_peerId++;
    }
    client.as_client("127.0.0.1", m_peers[cur_peerId].port.first);
    cur_peerId++;
    if (cur_peerId == m_peers.size() || (cur_peerId == m_peers.size() - 1 && cur_peerId == m_peerId)) {
        cur_peerId = 0;
    }
    m_lock.unlock();

    RequestVoteReply reply = client.call<RequestVoteReply>("requestVote", args).val();

    m_lock.lock();
    finishedVotes++;
    m_cond.notify_one();   //唤醒一个
    if (reply.term > m_currentTerm) {
        m_state = FOLLOWER;
        m_currentTerm = reply.term;
        m_votedFor = -1;
        readRaftState();
        m_lock.unlock();
        return;
    }
    if (reply.voteGranted) {
        recvVotes++;
    }
    m_lock.unlock();
    return;
}

void Raft::sendAppendEntries() {
    buttonrpc client;
    m_lock.lock();
    AppendEntriesArgs args;
    //跳过自己，也就是leader server(即m_peerId)
    if (cur_peerId == m_peerId) {
        cur_peerId++;
    }
    int clientPeerId = cur_peerId;
    client.as_client("127.0.0.1", m_peers[cur_peerId].port.second);
    cur_peerId++;

    args.term = m_currentTerm;
    args.leaderId = m_peerId;
    args.prevLogIndex = m_nextIndex[clientPeerId] - 1;
    args.leaderCommit = m_commitIndex;
    for (int i = args.prevLogIndex; i < m_logs.size(); i++) {//先记录sendLogs
        args.sendLogs += (m_logs[i].command + "," + to_string(m_logs[i].term) + ";");
    }
    if (args.prevLogIndex == 0) {  // 再更新prevLogTerm
        args.prevLogTerm = 0;
        if (m_logs.size() != 0) {
            args.prevLogTerm = m_logs[0].term;
        }
    } 
    else args.prevLogTerm = m_logs[args.prevLogIndex - 1].term;

    printf("[%d] -> [%d]'s prevLogIndex : %d, prevLogTerm : %d\n", m_peerId, clientPeerId, args.prevLogIndex, args.prevLogTerm);

    if (cur_peerId == m_peers.size() || (cur_peerId == m_peers.size() - 1 && cur_peerId == m_peerId)) {
        cur_peerId = 0;
    }
    m_lock.unlock();

    AppendEntriesReply reply = client.call<AppendEntriesReply>("appendEntries", args).val();

    m_lock.lock();
    if (reply.term > m_currentTerm) {
        m_state = FOLLOWER;
        m_currentTerm = reply.term;
        m_votedFor = -1;
        saveRaftState();
        m_lock.unlock();
        return;//FOLLOWER没必要维护nextIndex,成为leader会更新
    }
    if (reply.success) {
        m_nextIndex[clientPeerId] += getCmdAndTerm(args.sendLogs).size();
        m_matchIndex[clientPeerId] = m_nextIndex[clientPeerId] - 1;

        vector<int> tmpIndex = m_matchIndex;
        sort(tmpIndex.begin(), tmpIndex.end());
        int realMajorityMatchIndex = tmpIndex[tmpIndex.size() / 2];
        if (realMajorityMatchIndex > m_commitIndex && m_logs[realMajorityMatchIndex - 1].term == m_currentTerm) {
            m_commitIndex = realMajorityMatchIndex;
        }
    }
    if (!reply.success) {
        if(reply.conflict_term != -1){
            int leader_conflict_index = -1;
            for(int index = args.prevLogIndex; index >= 1; index--){
                if(m_logs[index - 1].term == reply.conflict_term){
                    leader_conflict_index = index;
                    break;
                }
            }
            if(leader_conflict_index != -1){
                m_nextIndex[clientPeerId] = leader_conflict_index + 1;
            }else{
                m_nextIndex[clientPeerId] = reply.conflict_term;
            }
        }else{
            m_nextIndex[clientPeerId] = reply.conflict_index + 1;
        }
    }
    saveRaftState();
    m_lock.unlock();
}

//Invoked by candidates to gather votes; RPC调用
RequestVoteReply Raft::requestVote(RequestVoteArgs args) {
    RequestVoteReply reply;
    reply.voteGranted = false;
    m_lock.lock();
    reply.term = m_currentTerm;

    //figure2 以及 选举约束中有提到这里的判断逻辑
    if (args.term < m_currentTerm) {
        m_lock.unlock();
        //表示不赞成给你投票 reply.voteGranted 为false
        return reply;
    }

    // Raft更喜欢拥有更高任期号记录的候选人
    if (args.term > m_currentTerm) {
        m_state = FOLLOWER;
        m_currentTerm = args.term;
        m_votedFor = -1;
    }

    // 如果votedFor为null或candidateId，并且候选人的日志至少与接收者的日志一样最新，则授予投票权
    if (m_votedFor == -1 || m_votedFor == args.candidateId) {
        m_lock.unlock();
        bool ret = checkLogUptodate(args.lastLogTerm, args.lastLogIndex);
        if (!ret) return reply;

        m_lock.lock();
        m_votedFor = args.candidateId;
        reply.voteGranted = true; //赞成投票给candidate
        printf("[%d] vote to [%d] at %d, duration is %d\n", m_peerId, args.candidateId, m_currentTerm, getMyduration(m_lastWakeTime));
        gettimeofday(&m_lastWakeTime, NULL);//更新m_lastWakeTime
    }
    saveRaftState();
    m_lock.unlock();
    return reply;
}

//Invoked by leader to replicate log entries; also used as heartbeat; RPC调用
AppendEntriesReply Raft::appendEntries(AppendEntriesArgs args) {
    vector<LogEntry> recvLog = getCmdAndTerm(args.sendLogs);
    AppendEntriesReply reply;
    m_lock.lock();
    reply.term = m_currentTerm;
    reply.success = false;
    reply.conflict_index = -1;
    reply.conflict_term = -1;
    
    //1.Reply false if term < currentTerm
    if (args.term < m_currentTerm) {
        m_lock.unlock();
        return reply;
    }

    if (args.term >= m_currentTerm) {
        

        m_currentTerm = args.term;
        m_state = FOLLOWER;
    }
    printf("[%d] recv append from [%d] at self term %d, send term %d, duration is %d\n", m_peerId, args.leaderId, m_currentTerm, args.term, getMyduration(m_lastWakeTime));
    //任何一条AppendEntries消息都会重置所有Raft节点的选举定时器
    gettimeofday(&m_lastWakeTime, NULL);//更新m_lastWakeTime

    int logSize = 0;
    //4.Append any new entries not already in the log
    if (m_logs.size() == 0) {
        for (const auto& log : recvLog) {
            m_logs.push_back(log);
        }
        logSize = m_logs.size();
        //5.If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if (m_commitIndex < args.leaderCommit) {
            m_commitIndex = min(args.leaderCommit, logSize);
        }
        m_lock.unlock();
        reply.success = true;
        return reply;
    }
    //2.Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
    if (m_logs.size() < args.prevLogIndex) {
        printf("[%d]'s logs.size : %d < [%d]'s prevLogIdx : %d\n", m_peerId, m_logs.size(), args.leaderId, args.prevLogIndex);
        reply.conflict_index = m_logs.size();//索引要加1
        m_lock.unlock();
        reply.success = false;
        return reply;
    }
    //3.If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
    if (args.prevLogIndex > 0 && m_logs[args.prevLogIndex - 1].term != args.prevLogTerm) {
        printf("[%d]'s prevLogterm : %d != [%d]'s prevLogTerm : %d\n", m_peerId, m_logs[args.prevLogIndex - 1].term, args.leaderId, args.prevLogTerm);
        reply.conflict_term = m_logs[args.prevLogIndex - 1].term;
        for (int index = 1; index <= args.prevLogIndex; index++) {
            if (m_logs[index - 1].term == reply.conflict_term) {
                //找到冲突term的第一个index,比索引要加1
                reply.conflict_index = index;
                break;
            }
        }
        m_lock.unlock();
        reply.success = false;
        return reply;
    }
    logSize = m_logs.size();
    for (int i = args.prevLogIndex; i < logSize; i++) {
        m_logs.pop_back();
    }
    for (const auto& log : recvLog) {
        m_logs.push_back(log);
    }
    saveRaftState();
    logSize = m_logs.size();
    if (m_commitIndex < args.leaderCommit) {
        m_commitIndex = min(args.leaderCommit, logSize);
    }
    for(const auto& log : m_logs) printf("%d ", log.term);
    printf(" [%d] sync success\n", m_peerId);
    m_lock.unlock();
    reply.success = true;
    return reply;
}

pair<int, bool> Raft::getState() {
    pair<int, bool> serverState;
    serverState.first = m_currentTerm;
    serverState.second = (m_state == LEADER);
    return serverState;
}

StartRet Raft::start(Operation op) {
    StartRet ret;
    m_lock.lock();
    RAFT_STATE state = m_state;
    if (state != LEADER) {
        m_lock.unlock();
        return ret;
    }
    ret.cmdIndex = m_logs.size();
    ret.curTerm = m_currentTerm;
    ret.isLeader = true;

    LogEntry log;
    log.command = op.getCmd();
    log.term = m_currentTerm;
    m_logs.push_back(log);
    m_lock.unlock();
    return ret;
}

void Raft::kill() {
    m_dead = true;
}

//判断是否最新日志(两个准则)，requestVote时会用到
//候选人的日志至少与接收者的日志一样最新，则授予投票权 参考figure2和笔记的选举约束 
bool Raft::checkLogUptodate(int lastLogTerm, int lastLogIndex) {
    m_lock.lock();
    if (m_logs.size() == 0) {
        m_lock.unlock();
        return true;
    }
    //候选人最后一条Log条目的任期号 大于 本地最后一条Log条目的任期号 投出赞成票
    if (lastLogTerm > m_logs.back().term) {
        m_lock.unlock();
        return true;
    }
    //候选人最后一条Log条目的任期号 等于 本地最后一条Log条目的任期号，且候选人的Log记录长度大于等于本地Log记录的长度；投出赞成票
    if (lastLogTerm == m_logs.back().term && lastLogIndex > m_logs.size()) {
        m_lock.unlock();
        return true;
    }
    m_lock.unlock();
    return false;
}

//返回的是微秒
int Raft::getMyduration(timeval lastTime) {
    struct timeval now;
    gettimeofday(&now, NULL);
    return ((now.tv_sec - lastTime.tv_sec) * 1000000 + (now.tv_usec - lastTime.tv_usec));
}

vector<LogEntry> Raft::getCmdAndTerm(string text) {
    vector<LogEntry> logs;
    int n = text.size();
    vector<string> str;
    string tmp = "";
    for (int i = 0; i < n; i++) {
        if (text[i] != ';') {
            tmp += text[i];
        }
        else {
            if (tmp.size() != 0) str.push_back(tmp);
            tmp = "";
        }
    }
    for (int i = 0; i < str.size(); i++) {
        tmp = "";
        int j = 0;
        for (; j < str[i].size(); j++) {
            if (str[i][j] != ',') {
                tmp += str[i][j];
            }
            else break;
        }
        string number(str[i].begin() + j + 1, str[i].end());
        int num = atoi(number.c_str());
        logs.push_back(LogEntry(tmp, num));
    }
    return logs;
}

//稍微解释下-200000us是因为让记录的m_lastBroadcastTime变早，这样在appendLoop中getMyduration(m_lastBroadcastTime)直接达到要求
//因为心跳周期是100000us
void Raft::setBroadcastTime() {
    gettimeofday(&m_lastBroadcastTime, NULL);
    printf("before : %ld, %ld\n", m_lastBroadcastTime.tv_sec, m_lastBroadcastTime.tv_usec);
    if (m_lastBroadcastTime.tv_usec >= 200000) {
        m_lastBroadcastTime.tv_usec -= 200000;
    }
    else {
        m_lastBroadcastTime.tv_sec -= 1;
        m_lastBroadcastTime.tv_usec += (1000000 - 200000);
    }
}

//实际的save在serialize()中
void Raft::saveRaftState() {
    m_persister.currentTerm = m_currentTerm;
    m_persister.votedFor = m_votedFor;
    m_persister.logs = m_logs;
    //在serialze()中加上一些固定的格式，再write到磁盘文件中
    serialize();
}

void Raft::serialize() {
    string str;
    str += to_string(m_persister.currentTerm) + ";" + to_string(m_persister.votedFor) + ";";
    for (const auto& log : m_persister.logs) {
        str += log.command + "," + to_string(log.term) + ".";
    }
    string filename = "persister-" + to_string(m_peerId);
    int fd = open(filename.c_str(), O_WRONLY | O_CREAT, 0664);
    if (fd == -1) {
        perror("open");
        exit(-1);
    }
    int len = write(fd, str.c_str(), str.size());
    if (len == -1) {
        perror("write");
        exit(-1);
    }
    close(fd);
}

//实际的read在deserialize()中
void Raft::readRaftState() {
    //deserialize()按照序列化的固定格式，进行反序列化
    bool ret = deserialize();
    if (!ret) return;
    m_currentTerm = m_persister.currentTerm;
    m_votedFor = m_persister.votedFor;

    //？？？这里有些疑问
    for (const auto& log : m_persister.logs) {
        m_logs.push_back(log);
    }
    printf("[%d]'s term : %d, votefor : %d, logs.size() : %d\n", m_peerId, m_currentTerm, m_votedFor, m_logs.size());
}

bool Raft::deserialize() {
    string filename = "persister-" + to_string(m_peerId);
    if (access(filename.c_str(), F_OK) == -1) return false;
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd == -1) {
        perror("open");
        exit(-1);
    }
    int length = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);
    char buf[length];
    bzero(buf, length);
    int len = read(fd, buf, length);
    if (len != length) {
        perror("read");
        exit(-1);
    }
    string content(buf);
    vector<string> persist;
    string tmp = "";
    for (int i = 0; i < content.size(); i++) {
        if (content[i] != ';') {
            tmp += content[i];
        }
        else {
            if (tmp.size() != 0) persist.push_back(tmp);
            tmp = "";
        }
    }
    //persist里面其实就只有三个string
    persist.push_back(tmp);
    this->m_persister.currentTerm = atoi(persist[0].c_str());
    this->m_persister.votedFor = atoi(persist[1].c_str());
    vector<string> log;
    vector<LogEntry> logs;
    tmp = "";
    //先分割 '.'
    for (int i = 0; i < persist[2].size(); i++) {
        if (persist[2][i] != '.') {
            tmp += persist[2][i];
        }
        else {
            if (tmp.size() != 0) log.push_back(tmp);
            tmp = "";
        }
    }
    //再分割 '，'
    for (int i = 0; i < log.size(); i++) {
        tmp = "";
        int j = 0;
        for (; j < log[i].size(); j++) {
            if (log[i][j] != ',') {
                tmp += log[i][j];
            }
            else break;
        }
        string number(log[i].begin() + j + 1, log[i].end());
        int num = atoi(number.c_str());
        logs.push_back(LogEntry(tmp, num));
    }
    this->m_persister.logs = logs;
    return true;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("loss parameter of peersNum\n");
        exit(-1);
    }
    int peersNum = atoi(argv[1]);
    if (peersNum % 2 == 0) {
        //必须传入奇数，这是raft集群的要求
        printf("the peersNum should be odd\n");
        exit(-1);
    }
    srand((unsigned)time(NULL));  // 设置随机数种子
    //根据peersNum 创建存储集群服务器的信息的数组peers
    vector<PeersInfo> peers(peersNum);
    for (int i = 0; i < peersNum; i++) {
        peers[i].peerId = i;
        //vote的RPC端口
        peers[i].port.first = COMMON_PORT + i;
        //append的RPC端口 
        peers[i].port.second = COMMON_PORT + i + peers.size();
    }
    //根据peers，创建并启动raft集群服务器
    Raft* raft = new Raft[peers.size()];
    for (int i = 0; i < peers.size(); i++) {
        raft[i].Make(peers, i);
    }

    //------------------------------test部分--------------------------
    usleep(400000);
    for (int i = 0; i < peers.size(); i++) {
        bool isLeader = raft[i].getState().second;
        if (isLeader) {
            for (int j = 0; j < 1000; j++) {
                Operation opera;
                opera.op = "put";
                opera.key = to_string(j);
                opera.value = to_string(j);
                raft[i].start(opera);
                usleep(50000);
            }
        }
        else continue;
    }
    usleep(400000);
    for (int i = 0; i < peers.size(); i++) {
        bool isLeader = raft[i].getState().second;
        if (isLeader) {
            ////kill后选举及心跳的线程会宕机，会产生新的leader，很久之后了，因为上面传了1000条日志
            raft[i].kill();
            break;
        }
    }
    //------------------------------test部分--------------------------
    while(1);
}