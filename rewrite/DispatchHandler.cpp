#include "DispatchHandler.h"
#include "SocketPool.h"

#include "proxygen/httpserver/RequestHandler.h"
#include "proxygen/httpserver/ResponseBuilder.h"


#include <chrono>
#include <thread>
#include <cstdlib>
#include <cstdio>
#include <string.h>
#include <sstream>

#include <errno.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h>

using namespace proxygen;

namespace DispatchService {

  DispatchHandler::DispatchHandler() {}

  void DispatchHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    _querynumber = _globalquerynumber.fetch_add(1);

    std::string resource = headers->getURL();

    if (resource == "/procedureRevenueSelect" || resource == "/procedureRevenueSelect/")
      _action = READ_QUERY;
    else if (resource == "/procedureRevenueInsert" || resource == "/procedureRevenueInsert/")
      _action = WRITE_QUERY;
    else if (resource == "/new_master")
      _action = NEW_MASTER;
    else if (resource == "/number_of_slaves_1")
      _action = SET_SLAVES_1;
    else if (resource == "/number_of_slaves_2")
      _action = SET_SLAVES_2;
    else if (resource == "/number_of_slaves_3")
      _action = SET_SLAVES_3;
    else if (resource == "/statistics")
      _action = STATISTICS;
    else if (resource == "/delay")
      _action = DELAY_QUERY;
    else
      _action = UNKNOWN_RESOURCE;
  }

  void DispatchHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {
    if (_body) {
      _body->prependChain(std::move(body));
    } else {
      _body = std::move(body);
    }
  }

  void DispatchHandler::onEOM() noexcept {
    if(_body)    
      _bodystr = IOBufToString(*_body);

    switch(_action) {
    case(READ_QUERY): 
      readQuery();
      break;
    case(WRITE_QUERY): 
      writeQuery();
      break;
    case(STATISTICS): 
      statistics();
      break;
    case(NEW_MASTER): 
      newMaster();
      break;
    case(SET_SLAVES_1):
      setSlaves(1);
      break;
    case(SET_SLAVES_2):
      setSlaves(2);
      break;
    case(SET_SLAVES_3):
      setSlaves(3);
      break;
    case(DELAY_QUERY):
      delayQuery();
      break;
    default:
      unknownResource();
    }
  }

  void DispatchHandler::onUpgrade(UpgradeProtocol protocol) noexcept {
    // no upgrade supported
  }

  void DispatchHandler::requestComplete() noexcept {
    delete this;
  }

  void DispatchHandler::onError(ProxygenError err) noexcept {
    delete this;
  }

  void DispatchHandler::newMaster() noexcept {
    if (!Settings::get_instance()->failoverdone && Settings::get_instance()->active_hosts_num > 1) {
      Settings::get_instance()->active_hosts_num--;
      Settings::get_instance()->current_master = 1;
      Settings::get_instance()->failoverdone = 1;
      Settings::get_instance()->number_of_hosts--;
    }

    ResponseBuilder(downstream_)
      .status(204, "No Content")
      .sendWithEOM();
  }

  void DispatchHandler::statistics() noexcept {
    std::cout << "statistics" << std::endl;
    std::stringstream ss;

    ss << "{\"read\": " << _readquerycounter.load()
       << ", \"write\": " << _writequerycounter.load()
       << ", \"timestamp\": " << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count() << "}\n";

    std::cout << "statistics" << std::endl;

    ResponseBuilder(downstream_)
      .status(200, "OK")
      .body(ss.str())
      .sendWithEOM();
  }

  void DispatchHandler::setSlaves(int number) {
    if ((number+1) < Settings::get_instance()->number_of_hosts)
      Settings::get_instance()->active_hosts_num = number+1;
    else 
      Settings::get_instance()->active_hosts_num = Settings::get_instance()->number_of_hosts;
    ResponseBuilder(downstream_)
      .status(204, "No Content")
      .sendWithEOM();
  }

  void DispatchHandler::readQuery() {
    int host = (_querynumber % Settings::get_instance()->active_hosts_num) + Settings::get_instance()->failoverdone;
    char response_buffer[65000]; // FIX: Magic number
    char* response_content;
    int response_offset = 0;
    int response_status = 0;
    int response_length = 0;

    int sock = SocketPool::get_instance()->getSocket(host);
    sendRequest(sock);
    getResponse(sock, response_buffer, &response_offset, &response_status, &response_content, &response_length);
    SocketPool::get_instance()->returnSocket(sock, host);
    std::string content = std::string(response_content, response_length);
    
    if (response_status != 200) {
      ResponseBuilder(downstream_)
	.status(response_status, "ERROR")
	.body(content)
	.sendWithEOM();
    } else {
      ResponseBuilder(downstream_)
	.status(200, "OK")
	.body(content)
	.sendWithEOM();
      _readquerycounter++;
    }
  }

  void DispatchHandler::writeQuery() {
    int host = Settings::get_instance()->current_master;
    char response_buffer[65000]; // FIX: Magic number
    char* response_content;
    int response_offset = 0;
    int response_status = 0;
    int response_length = 0;

    int sock = SocketPool::get_instance()->getSocket(host);
    sendRequest(sock);
    getResponse(sock, response_buffer, &response_offset, &response_status, &response_content, &response_length);
    SocketPool::get_instance()->returnSocket(sock, host);
    std::string content = std::string(response_content, response_length);
    
    if (response_status != 200) {
      ResponseBuilder(downstream_)
	.status(response_status, "ERROR")
	.body(content)
	.sendWithEOM();
    } else {
      ResponseBuilder(downstream_)
	.status(200, "OK")
	.body(content)
	.sendWithEOM();
      _writequerycounter++;
    }
  }

  void DispatchHandler::delayQuery() {
    int host = Settings::get_instance()->current_master;
    char response_buffer[65000]; // FIX: Magic number
    char* response_content;
    int response_offset = 0;
    int response_status = 0;
    int response_length = 0;

    int sock = SocketPool::get_instance()->getSocket(host);
    sendRequest(sock);
    getResponse(sock, response_buffer, &response_offset, &response_status, &response_content, &response_length);
    SocketPool::get_instance()->returnSocket(sock, host);
    std::string content = std::string(response_content, response_length);
    
    if (response_status != 200) {
      ResponseBuilder(downstream_)
	.status(response_status, "ERROR")
	.body(content)
	.sendWithEOM();
    } else {
      ResponseBuilder(downstream_)
	.status(200, "OK")
	.body(content)
	.sendWithEOM();
    }
  }

  void DispatchHandler::unknownResource() noexcept {
    ResponseBuilder(downstream_)
      .status(501, "Not Implemented")
      .body("Supported Operations:\n\
POST /procedureRevenueSelect/ JSON_QUERY\n\
POST /delay DELAY_QUERY\n\
POST /procedureRevenueInsert/ JSON_WRITE\n\
POST /new_master MASTER_IP MASTER_PORT\n\
POST /number_of_slaves NUMBER_OF_SLAVES\n")
      .sendWithEOM();
  }
  
  void DispatchHandler::sendRequest(int sockfd) {
    char* buf;
    if (_action == READ_QUERY)
      asprintf(&buf, DispatchService::http_post, "/procedureRevenueSelect/", strlen(_bodystr.c_str()), _bodystr.c_str());
    if (_action == WRITE_QUERY)
      asprintf(&buf, DispatchService::http_post, "/procedureRevenueInsert/", strlen(_bodystr.c_str()), _bodystr.c_str());
    // std::cout << "Sending: " << buf << std::endl;
    int err = send(sockfd, buf, strlen(buf), 0);
    // std::cout << "Sent byte: " << err << std::endl;
    free(buf);
  }

  int DispatchHandler::getResponse(int sockfd, char* buf, int *offset, int *status, char** content, int* content_length) {
    char* http_body_start = NULL;
    int recv_size = 0;
    int header_received = 0;
    int first_line_received = 0;

    while ((recv_size = read(sockfd, buf+(*offset), 65000-(*offset))) > 0) { // FIX: Magic number
      *offset += recv_size;
      
      // check status
      if (!first_line_received) {
	char *hit_ptr;
	hit_ptr = strnstr_(buf, "\n", *offset, 1);
	if (hit_ptr == NULL) {
	  continue;
	}
	first_line_received = 1;
	int n;
	if (!((n = sscanf(buf, "HTTP/1.1 %d", status)) == 1)) {
	  return -1;
	}
      }
      
      // get whole header
      if (!header_received) {
	char* hit_ptr;
	hit_ptr = strnstr_(buf, "\r\n\r\n", *offset, 4);
	http_body_start = hit_ptr + 4;
	if (hit_ptr == NULL) {
	  continue;
	}
	header_received = 1;
	*content_length = get_content_length(buf, *offset);
      }

      // get content
      if (http_body_start != NULL) {
	if (((http_body_start - buf) + *content_length) == *offset) {
	  *content = http_body_start;
	  return 0;
	}
      }

      if (recv_size <= 0) {
	return -1;
      }

      if (*content == NULL) {
	return -1;
      }
    }
  }

  std::string DispatchHandler::IOBufToString(const folly::IOBuf& buf) {
    std::string str;
    Cursor cursor(&buf);
    std::pair<const uint8_t*, size_t> p;
    while ((p = cursor.peek()).second) {
      str.append(reinterpret_cast<const char*>(p.first), p.second);
      cursor.skip(p.second);
    }
    return str;
  }

  char* DispatchHandler::strnstr_(const char *haystack, const char *needle, size_t len_haystack, size_t len_needle) {
    if (len_haystack == 0) return (char *)haystack; /* degenerate edge case */
    if (len_needle == 0) return (char *)haystack; /* degenerate edge case */
    while ((haystack = strchr(haystack, needle[0]))) {
      if (!strncmp(haystack, needle, len_needle)) return (char *)haystack;
      haystack++; }
    return NULL;
  }

  int DispatchHandler::get_content_length1(const char *buf, const int size, const char *lengthname) {
    const char *hit_ptr;
    int content_length;
    hit_ptr = strnstr_(buf, lengthname, size, 15);
    if (hit_ptr == NULL) {
      return -1;
    }
    char format [50];
    strcpy(format,lengthname);
    strcat(format," %d");
    fflush(stdout);
    if (sscanf(hit_ptr, format, &content_length) != 1) {
      return -1;
    }
    return content_length;
  }

  int DispatchHandler::get_content_length(const char *buf, const int size) {
    int res = get_content_length1(buf, size, "Content-Length:");
    if (res == -1)
      res = get_content_length1(buf, size, "Content-length:");
    if (res == -1)
      res = get_content_length1(buf, size, "content-length:");
    return res;
  }



  std::atomic<int> DispatchHandler::_readquerycounter(0);
  std::atomic<int> DispatchHandler::_writequerycounter(0);
  std::atomic<int> DispatchHandler::_globalquerynumber(0);

}
