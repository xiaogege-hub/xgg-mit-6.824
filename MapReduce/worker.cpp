#include <dlfcn.h>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>   
#include <dirent.h>
#include <unordered_map>
#include "./buttonrpc-master/buttonrpc.hpp"
using namespace std;

#define MAX_REDUCE_NUM 15

struct KeyValue {
 public:
  string key;
  string value;
};

// 定义的两个函数指针用于动态加载动态库里的map和reduce函数
// 本来这里打算用function代替函数指针，更清晰些，但是void*没法转为function，故放弃
// using MapFunc = function<vector<KeyValue>(KeyValue)>;
// using ReduceFunc = function<vector<string>(vector<KeyValue>, int)>;
typedef vector<KeyValue> (*MapFunc)(KeyValue kv);
typedef vector<string> (*ReduceFunc)(vector<KeyValue> kvs);
MapFunc mapF;
ReduceFunc reduceF;

class Worker {
public:    
    void removeTmpFiles();   // 删除所有写入中间值的临时文件

    void removeOutputFiles();// 删除最终输出文件，用于程序第二次执行时清除上次保存的结果

    void mapWorker();

    void reduceWorker();

private:
    KeyValue getContent(char* file); //取得key:filename, value:content 的kv对作为map任务的输入

    vector<KeyValue> myShuffle(int reduceTaskId);//对于一个ReduceTask，获取所有相关文件并将value的list以string写入vector

    vector<string> getAllfile(string path, int op);//获取对应reduce编号的所有中间文件

    void writeMap(const vector<KeyValue>& kvs, int mapTaskId);//创建每个map任务对应的不同reduce号的中间文件,并调用 ->writeKV 写入磁盘

    void writeKV(int fd, const KeyValue& kv);//将map任务产生的中间值写入临时文件

    void writeReduce(int fd, vector<string>& str);////将reduce的最终结果写入磁盘

    vector<string> split(string text, char op);//以char类型的op为分割拆分字符串

    string split(string text);//以逗号为分割拆分字符串

    int ihash(string str);//对每个字符串求hash找到其对应要分配的reduce线程

public:
    // 定义master分配给自己的map和reduce任务数，实际无所谓随意创建几个，我是为了更方便测试代码是否ok
    int map_task_num;

    int reduce_task_num;

    //互斥锁与条件变量，保护共享数据
    mutex map_mutex;

    condition_variable cond;

private:
    //给每个map线程分配的任务ID，用于写中间文件时的命名
    int MapId = 0;

    int fileId = 0;

    //人为让特定任务超时，用于后面mapWorker工作线程中测试超时重转发
    int disabledMapId = 0;      //用于人为让特定map任务超时的Id

    int disabledReduceId = 0;   //用于人为让特定reduce任务超时的Id
};

void Worker::mapWorker() {
    //1、初始化client连接用于后续RPC;各个线程获取自己唯一的mapTaskId
    buttonrpc client;
    client.as_client("127.0.0.1", 5555);
    map_mutex.lock();
    int mapTaskId = MapId++;
    map_mutex.unlock();
    bool ret = false;
    while(1) {
        //2、通过RPC从Master获取任务
        //若工作完成直接退出map的worker线程
        ret = client.call<bool>("isMapDone").val();
        if (ret) {
            cond.notify_all();//notify_one也可以，本身就一个主线程在等
            return;
        }
        //通过RPC返回值取得任务，在map中即为文件名
        string taskTmp = client.call<string>("assignMapTask").val();
        if (taskTmp == "empty") continue;//为什么不能直接return？
        printf("%d get the map task : %s\n", mapTaskId, taskTmp.c_str());

        //------------------自己写的测试超时重转发的部分-------------------
        //这里是强行模仿线程宕机
        //注：需要对应master所规定的map数量，因为是1，3，5被置为disabled，相当于第2，4，6个拿到任务的线程宕机
        //若只分配两个map的worker，即0工作，1宕机，我设的超时时间比较长且是一个任务拿完在拿一个任务，所有1的任务超时后都会给到0
        map_mutex.lock();
        if (disabledMapId == 1 || disabledMapId == 3 || disabledMapId == 5) {
          disabledMapId++;
          map_mutex.unlock();
          printf("%d recv map task : %s is stop\n", mapTaskId, taskTmp.c_str());
          while (1) {
            sleep(2);
          }
        }
        else {
          disabledMapId++;
        }
        map_mutex.unlock();
        //------------------自己写的测试超时重转发的部分------------------

        //3、拆分任务，任务返回为文件path及map任务编号，将filename及content封装到kv的key及value中
        char task[taskTmp.size() + 1];
        strcpy(task, taskTmp.c_str());
        KeyValue kv = getContent(task);

        // 4、执行map函数，然后将中间值写入本地
        vector<KeyValue> kvs = mapF(kv);    //这里完成了拆分
        writeMap(kvs, mapTaskId);

        //5、发送RPC给master告知任务已完成，修改map状态
        printf("%d finish the map task : %s\n", mapTaskId, taskTmp.c_str());
        client.call<void>("setMapState", taskTmp);
    }
}

void Worker::reduceWorker() {
    //1、初始化client连接用于后续RPC
    buttonrpc client;
    client.as_client("127.0.0.1", 5555);
    bool ret = false;
    while (1) {
        //2、通过RPC从Master获取任务
        //若工作完成直接退出reduce的worker线程
        ret = client.call<bool>("isReduceDone").val();
        if (ret) {
          return;
        }
        //通过RPC返回值取得任务,在reduce中即为编号
        int reduceTaskId = client.call<int>("assignReduceTask").val();
        if (reduceTaskId == -1) continue;
        printf("%ld get the reduce task %d\n", this_thread::get_id(), reduceTaskId);

        //----人为设置的crash线程，会导致超时，用于超时功能的测试------
        map_mutex.lock();
        if(disabledReduceId == 1 || disabledReduceId == 3 || disabledReduceId == 5) {
          disabledReduceId++;
          map_mutex.unlock();
          printf("recv reduce task %d reduceTaskId is stop in %ld\n", reduceTaskId, this_thread::get_id());
          while (1) {
            sleep(2);
          }
        }
        else {
          disabledReduceId++;
        }
        map_mutex.unlock();
        //----人为设置的crash线程，会导致超时，用于超时功能的测试------

        //3、取得reduce任务，各个线程读取各自reduceTaskId对应的中间文件，shuffle后调用reduceFunc进行reduce处理，shuffle完，kvs中每个元素的形式为"abc 11111"
        vector<KeyValue> kvs = myShuffle(reduceTaskId);

        //4、执行reduce函数，并记录结果在str中 str中是形如"abc 4"的字符串
        vector<string> ret = reduceF(kvs);//ret是各个单词出现的次数
        vector<string> str;
        for (int i = 0; i < kvs.size(); i++) {
            string ans = kvs[i].key + " " + ret[i]; 
            str.push_back(ans);//记录结果
        }

        //5、生成最终文件,并将reduce的最终结果写入本地
        string filename = "mr-out-" + to_string(reduceTaskId);
        int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0664);
        writeReduce(fd, str);
        close(fd);

        //6、发送RPC给master告知任务已完成，修改reduce状态
        printf("%ld finish the task%d\n", this_thread::get_id(), reduceTaskId);
        client.call<void>("setReduceState",reduceTaskId);
    }
}

//取得  key:filename, value:content 的kv对作为map任务的输入
KeyValue Worker::getContent(char* file) {
  int fd = open(file, O_RDONLY);
  int length = lseek(fd, 0, SEEK_END);//通过lseek计算文件的字节数
  lseek(fd, 0, SEEK_SET);             //移动到文件开始位置
  char buf[length];
  bzero(buf, length);
  int len = read(fd, buf, length);
  if (len != length) {
    perror("read error");
    exit(-1);
  }
  KeyValue kv;
  kv.key = string(file);
  kv.value = string(buf);
  close(fd);
  return kv;
}

//对于一个ReduceTask，获取reduceTaskId对应的中间文件，并将value的list以string写入vector，vector中每个元素的形式为"abc 11111",记录在vector中return
vector<KeyValue> Worker::myShuffle(int reduceTaskId){
    string path;
    vector<string> strs;
    strs.clear();
    //通过getAllfile获取reduceTaskId对应的文件名，即得到了各个线程对应的reduce task
    vector<string> filename = getAllfile(".", reduceTaskId);
    unordered_map<string, string> hash;
    for (int i = 0; i < filename.size(); i++) {
        path = filename[i];
        char text[path.size() + 1];
        strcpy(text, path.c_str());
        //获得一个kv，key对应文件名，value对应文件内容
        KeyValue kv = getContent(text);
        string content = kv.value;
        //把content的内容以空格为分割，切割成一个个stirng 形式如："our,1"
        vector<string> retStr = split(content, ' ');
        strs.insert(strs.end(), retStr.begin(), retStr.end());
    }
    //去除"our,1"中的",1" , 并加入到hash中 (strs中依然是"our,1"形式的字符串)
    for (const auto& str : strs) {
        string s = split(str);//得到一个完整的单词，比如"our"
        hash[s] += "1";
    }
    vector<KeyValue> retKvs;
    KeyValue tmpKv;
    for (const auto& a : hash) {
        tmpKv.key = a.first;
        tmpKv.value = a.second;
        retKvs.push_back(tmpKv);
    }
    sort(retKvs.begin(), retKvs.end(), [](const KeyValue& kv1, const KeyValue& kv2) { return kv1.key < kv2.key;} );
    return retKvs;
}

//获取对应reduce编号的所有中间文件 中间文件格式：mr-0-1
vector<string> Worker::getAllfile(string path, int op) {
    DIR* dir = opendir(path.c_str());
    vector<string> ret;
    if (dir == NULL) {
        printf("[ERROR] %s is not a directory or not exist!", path.c_str());
        return ret;
    }
    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {//readdir会依次读dir下的每一个文件
        int len = strlen(entry->d_name);
        int oplen = to_string(op).size();
        if (len - oplen < 5) continue;//说明不是我们生成的中间文件
        string filename(entry->d_name);
        if (!(filename[0] == 'm' && filename[1] == 'r' && filename[len - oplen - 1] == '-')) continue;//说明不是我们生成的中间文件
        string cmp_str = filename.substr(len - oplen, oplen);
        if (cmp_str == to_string(op)) {//确保是对应reduce编号的；
            ret.push_back(entry->d_name);
        }
    }
    closedir(dir);
    return ret;
}

//创建每个map任务对应的不同reduce号的中间文件，并调用->writeKV写入磁盘
void Worker::writeMap(const vector<KeyValue>& kvs, int mapTaskId) {
  for (const auto& kv : kvs) {
    int reduceTaskId = ihash(kv.key);
    string path;
    path = "mr-" + to_string(mapTaskId) + "-" + to_string(reduceTaskId);
    int ret = access(path.c_str(), F_OK);
    if (ret == -1) {
        int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0664);
        writeKV(fd, kv);
    }
    else if (ret == 0) {
        int fd = open(path.c_str(), O_WRONLY | O_APPEND);
        writeKV(fd, kv);
    }
  }
}

//将map任务产生的中间值写入临时文件,便于后面reduce
void Worker::writeKV(int fd, const KeyValue& kv) {
    string tmp = kv.key + ",1 ";
    int len = write(fd, tmp.c_str(), tmp.size());
    if (len == -1) {
        perror("write error");
        exit(-1);
    }
    close(fd);
}

//将reduce的最终结果写入磁盘 
void Worker::writeReduce(int fd, vector<string>& str) {
    int len = 0;
    char buf[2];
    sprintf(buf, "\n");
    for (const auto& s : str) {
        len = write(fd, s.c_str(), s.size());
        write(fd, buf, strlen(buf));    //加入换行符
        if (len == -1) {
            perror("write error");
            exit(-1);
        }
    }
}

// 删除所有写入中间值的临时文件
void Worker::removeTmpFiles() {
  string path;
  for (int i = 0; i < map_task_num; i++) {
    for (int j = 0; j < reduce_task_num; j++) {
      path = "mr-" + to_string(i) + "-" + to_string(j);
      int ret = access(path.c_str(), F_OK);//判断文件是否存在
      if (ret == 0) remove(path.c_str());
    }
  }
}

// 删除最终输出文件，用于程序第二次执行时清除上次保存的结果
void Worker::removeOutputFiles() {
    string path;
    for (int i = 0; i < MAX_REDUCE_NUM; i++) {
        path = "mr-out-" + to_string(i);
        int ret = access(path.c_str(), F_OK);
        if (ret == 0) remove(path.c_str());
    }
}

// 以char类型的op拆分字符串
vector<string> Worker::split(string text, char op) {
    int n = text.size();
    vector<string> str;
    string tmp = "";
    for (int i = 0; i < n; i++) {
        if (text[i] != op) {
            tmp += text[i];
        }
        else {
            if (tmp.size() != 0) str.push_back(tmp);
            tmp = "";
        }
    }
    return str;
}

//以逗号为分割拆分字符串  这里的字符串是中间文件中的，格式：our,1
string Worker::split(string text) {
    string tmp = "";
    for (int i = 0; i < text.size(); i++) {
        if (text[i] != ',') {
            tmp += text[i];
        }
        else break;//这样的话顺便把后面的1也给除去了
    }
    return tmp;
}

//对每个字符串求hash找到其对应要分配的reduce线程
int Worker::ihash(string str) {
  int sum = 0;
  for (int i = 0; i < str.size(); i++) {
    sum += (str[i] - '0');
  }
  return sum % reduce_task_num;
}

int main() {
  // 运行时从动态库中加载map及reduce函数(根据实际需要的功能加载对应的Func)
  void *handle = dlopen("./libmrFunc.so", RTLD_LAZY);
  if (!handle) {
    cerr << "Cannot open library: " << dlerror() << endl;
    exit(-1);
  }
  mapF = (MapFunc)dlsym(handle, "mapF");
  if (!mapF) {
    cerr << "Cannot load mapF : " << dlerror() << endl;
    dlclose(handle);
    exit(-1);
  }
  reduceF = (ReduceFunc)dlsym(handle, "reduceF");
  if (!reduceF) {
    cerr << "Cannot load reduceF : " << dlerror() << endl;
    dlclose(handle);
    exit(-1);
  }

  // 作为RPC请求端
  buttonrpc client;
  client.as_client("127.0.0.1", 5555);
  client.set_timeout(5000);
  Worker worker;
  worker.map_task_num = client.call<int>("getMapNum").val();
  worker.reduce_task_num = client.call<int>("getReduceNum").val();
  worker.removeTmpFiles();     // 若有，则清理上次输出的中间文件
  worker.removeOutputFiles();  // 清理上次输出的最终文件

  // 创建多个map及reduce的worker线程
  vector<thread> tidMap;
  vector<thread> tidReduce;
  // 先创建处理map任务的work线程
  for (int i = 0; i < worker.map_task_num; i++) {
    tidMap.emplace_back(thread(&Worker::mapWorker, &worker));
    tidMap[i].detach();
  }
  {
    unique_lock<mutex> lock(worker.map_mutex);
    //map任务处理完了，再处理reduce任务；所以做完前先阻塞在这里
    worker.cond.wait(lock);
  }
  // 再创建处理reduce任务的work线程
  for (int i = 0; i < worker.reduce_task_num; i++) {
    tidReduce.emplace_back(thread(&Worker::reduceWorker, &worker));
    tidReduce[i].detach();
  }
  while (1) {
      if (client.call<bool>("isReduceDone").val()) {
        break;
      }
      sleep(1);
  }

  // 任务完成后,清理中间文件，关闭打开的动态库，释放资源
  worker.removeTmpFiles();
  dlclose(handle);

  return 0;
}