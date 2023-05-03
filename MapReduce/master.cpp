#include <iostream>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unistd.h> 
#include "./buttonrpc-master/buttonrpc.hpp"
using namespace std;

#define MAP_TASK_TIMEOUT 3
#define REDUCE_TASK_TIMEOUT 5

class Master {
public:
  //带缺省值的有参构造，也可通过命令行传参指定，我偷懒少打两个数字直接放构造函数里
  Master(int mapNum = 8, int reduceNum = 8);  

  //从argv[]中获取待处理的文件名与文件数,文件名记录在m_list中，文件数记录在fileNum中
  void GetAllFile(char *file[], int argc);  

  int getMapNum() { return m_mapNum; }

  int getReduceNum() { return m_reduceNum; }

  string assignMapTask();  // 分配map任务的函数，RPC

  int assignReduceTask();  // 分配reduce任务的函数，RPC

  void setMapState(string filename);  //设置特定map任务完成的函数，RPC

  void setReduceState(int taskIndex); //设置特定reduce任务完成的函数，RPC

  bool isMapDone();        // 检验所有map任务是否完成，RPC

  bool isReduceDone();     // 判断reduce任务是否已经完成

private:
  void waitMap(string filename);

  void waitReduce(int reduceIdx);

  void waitMapTask();         //回收map的定时线程

  void waitReduceTask();      //回收reduce的定时线程

  void waitTime(char op);     //用于定时的线程

private:
  mutex m_assign_lock;      // 保护共享数据的锁

  list<char*> m_list;       // 记录待处理的文件名，即所有map任务的工作队列

  vector<int> reduceIndex;  // 所有reduce任务的工作队列

  int fileNum;              // 记录待处理的文件数

  int m_mapNum;

  int m_reduceNum;

  unordered_map<string, int> finishedMapTask;   // 存放所有完成的map任务对应的文件

  unordered_map<int, int> finishedReduceTask;   // 存放所有完成的reduce任务对应的reduce编号

  vector<string> runningMapWork;  // 正在处理的map任务，分配出去就加到这个队列，用于判断超时处理重发

  vector<int> runningReduceWork;  // 正在处理的reduce任务，分配出去就加到这个队列，用于判断超时处理重发

  int curMapIndex;                // 当前处理第几个map任务

  int curReduceIndex;             // 当前处理第几个reduce任务
};

Master::Master(int mapNum = 8, int reduceNum = 8)
    :m_mapNum(mapNum), m_reduceNum(reduceNum) {
  m_list.clear();
  reduceIndex.clear();
  finishedMapTask.clear();
  finishedReduceTask.clear();
  runningMapWork.clear();
  runningReduceWork.clear();
  curMapIndex = 0;
  curReduceIndex = 0;
  if(m_mapNum <= 0 || m_reduceNum <= 0){
        throw std::exception();
  }
  for(int i = 0; i < reduceNum; i++){
        reduceIndex.emplace_back(i);
  }
}

void Master::GetAllFile(char *file[], int argc) {
  for (int i = 1; i < argc; i++) {
    m_list.emplace_back(file[i]);
  }
  fileNum = argc - 1;
}

// map阶段的worker只需要拿到对应的文件名就可以执行map
string Master::assignMapTask() {
  if (isMapDone()) return "empty";
  if (!m_list.empty()) {
    char *task;
    {
      lock_guard<mutex> lock(m_assign_lock);
      // 从工作队列取出一个待map的文件名
      task = m_list.back();  
      m_list.pop_back();
    }
    //调用waitMap将取出的任务加入正在运行的map任务队列并等待计时线程
    waitMap(string(task));  
    return string(task);
  }
  return "empty";
}

int Master::assignReduceTask() {
    if (isReduceDone()) return -1;
    if (!reduceIndex.empty()) {
        int reduceIdx = 0;
        {
            lock_guard<mutex> lock(m_assign_lock);
            //取出reduce编号
            reduceIdx = reduceIndex.back();
            reduceIndex.pop_back();
        }
        //调用waitReduce将取出的任务加入正在运行的reduce任务队列并等待计时线程
        waitReduce(reduceIdx);    
        return reduceIdx;
    }
    return -1;
}

void Master::setMapState(string filename) {
    {
        lock_guard<mutex> lock(m_assign_lock);
        //通过worker的RPC调用修改map任务的完成状态
        finishedMapTask[filename] = 1;
    }
    return;
}

void Master::setReduceState(int taskIndex) {
    {
        lock_guard<mutex> lock(m_assign_lock);
        //通过worker的RPC调用修改reduce任务的完成状态
        finishedReduceTask[taskIndex] = 1;
    }
    return;
}

bool Master::isMapDone() {
  // 当统计完成map任务的hashmap大小达到文件数，map任务完成
  int len = 0;
  {
    lock_guard<mutex> lock(m_assign_lock);
    len = finishedMapTask.size();
  }
  return len == fileNum;
}

bool Master::isReduceDone() {
    //当统计reduce的hashmap若是达到reduceNum，reduce任务完成
    int len = 0;
    {
        lock_guard<mutex> lock(m_assign_lock);
        len = finishedReduceTask.size();
    }
    return len == m_reduceNum;
}

//将取出的任务加入正在运行的map任务队列并等待计时线程
void Master::waitMap(string filename) {
  {
    lock_guard<mutex> lock(m_assign_lock);
    // 将分配出去的map任务加入正在运行的工作队列runningMapWork
    runningMapWork.push_back(filename);  
  }
  // 创建一个用于回收计时线程并处理超时逻辑的线程
  thread t(&Master::waitMapTask);
  t.detach();
}

//将取出的任务加入正在运行的reduce任务队列并等待计时线程
void Master::waitReduce(int reduceIdx) {
    {
        lock_guard<mutex> lock(m_assign_lock);
        // 将分配出去的reduce任务加入正在运行的工作队列runningReduceWork
        runningReduceWork.push_back(reduceIdx);
    }
    // 创建一个用于回收计时线程及处理超时逻辑的线程
    thread t(&Master::waitReduceTask);
    t.detach();
}

void Master::waitMapTask() {
    //标记这是map任务
    char op = 'm';
    thread t_time(&Master::waitTime, op);
    //join方式回收实现超时后解除阻塞
    t_time.join();

    unique_lock<mutex> lock(m_assign_lock);
    //因为master需要实现map任务的超时重分配,若超时后在
    //对应的hashmap中没有该map任务完成的记录，重新将该任务加入工作队列m_list
    if (!finishedMapTask.count(runningMapWork[curMapIndex])) {
        printf("filename: %s is timeout\n", runningMapWork[curMapIndex].c_str());
        const char* text = runningMapWork[curMapIndex].c_str();
        m_list.push_back((char*)text);
        curMapIndex++;
        lock.unlock();
        return;
    }
    printf("filename: %s is finished at idx : %d\n", runningMapWork[curMapIndex].c_str(), curMapIndex);
    curMapIndex++;
    lock.unlock();
    return;
}

void Master::waitReduceTask() {
    //标记这是reduce任务
    char op = 'r';
    thread t_time(&Master::waitTime, op);
    t_time.join();

    unique_lock<mutex> lock(m_assign_lock);
    //因为master需要实现reduce任务的超时重分配,若超时后在对应的hashmap
    //中没有该reduce任务完成的记录，将该任务重新加入工作队列reduceIndex
    if (!finishedReduceTask.count(runningReduceWork[curReduceIndex])) {
        for(auto a : reduce->m_list) printf(" before insert %s\n", a);
        reduceIndex.emplace_back(runningReduceWork[curReduceIndex]);
        printf("reduce %d is timeout\n", runningReduceWork[curReduceIndex]);
        curReduceIndex++;
        for(auto a : reduce->m_list) printf(" after insert %s\n", a);
        lock.unlock();
        return;
    }
    printf("%d reduce is completed\n", runningReduceWork[curReduceIndex]);
    curReduceIndex++;
    lock.unlock();
    return;
}

//区分map任务还是reduce任务，执行不同时间计时的计时线程
void Master::waitTime(char op) {
    if (op == 'm') {
        sleep(MAP_TASK_TIMEOUT);
    }
    else if (op == 'r') {
        sleep(REDUCE_TASK_TIMEOUT);
    }
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    cout << "missing parameter! The format is ./Master pg*.txt" << endl;
    exit(-1);
  }
  buttonrpc server;
  server.as_server(5555);
  Master master(13, 9);
  master.GetAllFile(argv, argc);

  server.bind("getMapNum", &Master::getMapNum, &master);
  server.bind("getReduceNum", &Master::getReduceNum, &master);
  server.bind("assignMapTask", &Master::assignMapTask, &master);
  server.bind("assignReduceTask", &Master::assignReduceTask, &master);
  server.bind("setMapState", &Master::setMapState, &master);
  server.bind("setReduceState", &Master::setReduceState, &master);
  server.bind("isMapDone", &Master::isMapDone, &master);
  server.bind("isReduceDone", &Master::isReduceDone, &master);

  server.run();
  return 0;
}