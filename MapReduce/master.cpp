#include <iostream>
#include "./buttonrpc-master/buttonrpc.hpp"

using namespace std;

int main(int argc, char* argv[]) { 
    if (argc < 2) {
        cout << "missing parameter! The format is ./Master pg*.txt" << endl;
        exit(-1);
    }
    buttonrpc server;
    
    
    return 0; 
}