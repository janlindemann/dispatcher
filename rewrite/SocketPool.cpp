#include "SocketPool.h"
#include "Settings.h"

#include <iostream>
#include <string.h>
#include <errno.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <queue>

namespace DispatchService {

  SocketPool::SocketPool() {
    _mutexes = new std::mutex*[Settings::get_instance()->number_of_hosts];
     for(int i=0; i<Settings::get_instance()->number_of_hosts; ++i) {
       _mutexes[i] = new std::mutex;
     }

     _sockets = new std::queue<int>*[Settings::get_instance()->number_of_hosts];
    for(int i=0; i<Settings::get_instance()->number_of_hosts; ++i) {
      _sockets[i] = new std::queue<int>;
    }
  }

   int SocketPool::getSocket(int host) {
    // returns a socket connected to host. 
    // if no open socket to host exists a new socket is created and added
    // to the appropriate queue
    int sock = -1;

    _mutexes[host]->lock();
    
    if (_sockets[host]->empty()) {
      struct sockaddr_in dest;
    
      if (( sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
	std::cout << "Error: Could not create new socket." << std::endl;
	return -1;
      }

      //setsockopt(sockfd,SOL_SOCKET,SO_REUSEADDR,&i,sizeof(i));
    
      bzero(&dest, sizeof(dest));
      dest.sin_family = AF_INET;
      dest.sin_addr.s_addr = inet_addr(Settings::get_instance()->hosts[host][0].c_str());
      dest.sin_port = htons(std::atoi(Settings::get_instance()->hosts[host][1].c_str()));
      
      if (connect(sock, (struct sockaddr*)&dest, sizeof(dest)) != 0) {
	std::cout << "ERROR: Could not connect to host: " 
		  << Settings::get_instance()->hosts[host][0] 
		  << ":" << Settings::get_instance()->hosts[host][1]
		  <<std::endl;
	std::cout << "Error: " << strerror(errno) << std::endl;
      }
    } else {
      sock = _sockets[host]->front();
      _sockets[host]->pop();
    }

    _mutexes[host]->unlock();

    return sock;
  }

   void SocketPool::returnSocket(int sockfd, int host) {
    _mutexes[host]->lock();
    
    _sockets[host]->push(sockfd);
    
    _mutexes[host]->unlock();
  }

}    
