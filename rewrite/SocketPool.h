#pragma once

#include <queue>
#include <mutex>

namespace DispatchService {
  
  class SocketPool {
  public:
    static SocketPool* get_instance() {
      static SocketPool* instance;
      if (instance == 0) {
	instance = new SocketPool();
      }
      return instance;
    };
    int getSocket(int host);
    void returnSocket(int sockfd, int host);
    
  private:
    SocketPool();
    
    std::queue<int>** _sockets;
    int _number_of_sockets;
    std::mutex** _mutexes;
  };
  
}
