#include <iostream>
#include <vector>
#include <string>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
using namespace std;

struct KeyValue {
public:
    string key;
    string value;
};

/**
 * @brief 自己写的字符串按照单词分割函数，因为mapReduce中经常用到
 * 
 * @param text 传入的文本内容，数据量极大
 * @param length 传入的字符串长度
 * @return vector<string> 返回各个分割后的单词列表
 */
vector<string> split(char* text, int length) {
    vector<string> str;
    string tmp = "";
    for (int i = 0; i < length; i++) {
        if ((text[i] >= 'A' && text[i] <= 'Z') || (text[i] >= 'a' && text[i] <= 'z')) {
            tmp += text[i];
        }
        else {
            if (tmp.size() != 0) str.push_back(tmp);
            tmp = "";
        }
    }
    return str;
}

/**
 * @brief mapFunc，需要打包成动态库，并在worker中通过dlopen以及dlsym运行时加载
 * @param kv 将文本按单词划分并以出现次数代表value长度存入keyValue
 * @return 类似{"my 1", "you 1"，"my 1"} 
 */
extern "C" vector<KeyValue> mapF(KeyValue kv) {
    vector<KeyValue> kvs;
    int len = kv.value.size();
    char content[len + 1];
    strcpy(content, kv.value.c_str());
    vector<string> str = split(content, len); //把文本内容拆分成一个个单词
    for (const auto& s : str) {
        KeyValue tmp;
        tmp.key = s;
        tmp.value = "1";
        kvs.emplace_back(tmp);
    }
    return kvs;
}

/**
 * @brief redecuFunc，也是动态加载，输出对特定keyValue的reduce结果
 * @param kvs kvs中每个元素的形式为"abc 11111"
 * @return vector<string> 各个单词出现的次数
 */
extern "C" vector<string> reduceF(vector<KeyValue> kvs) {
    vector<string> str;
    for (const auto& kv : kvs) {
        //统计每一个kv中出现了几个1
        int count = kv.value.size();
        str.push_back(to_string(count));
    }
    return str;
}
