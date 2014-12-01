#pragma once
#include "proxygen/httpserver/RequestHandler.h"
#include "Settings.h"

#include <folly/Memory.h>
#include <folly/io/IOBuf.h>
#include <folly/io/Cursor.h>

#include <atomic>

namespace proxygen {
  class ResponseHandler;
}

namespace DispatchService {

  enum Actions {READ_QUERY, WRITE_QUERY, NEW_MASTER, SET_SLAVES_1, SET_SLAVES_2, SET_SLAVES_3, STATISTICS, DELAY_QUERY, UNKNOWN_RESOURCE};
  static char http_post[] = "POST %s HTTP/1.1\r\n\
Content-Length: %d\r\n\
Connection: Keep-Alive\r\n\r\n\
%s";
  
  class DispatchHandler : public proxygen::RequestHandler {
  public:
    explicit DispatchHandler();
    
    void onRequest(std::unique_ptr<proxygen::HTTPMessage>) noexcept override;
    
    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError err) noexcept override;

  private:
    void newMaster() noexcept;
    void statistics() noexcept;
    void readQuery();
    void writeQuery();
    void delayQuery();
    void setSlaves(int number);
    void unknownResource() noexcept;

    void sendRequest(int sockfd);
    int getResponse(int sockfd, char* buf, int *offset, int *status, char** content, int *content_length);

    std::string IOBufToString(const folly::IOBuf& buf);
    char* strnstr_(const char* haystack, const char* needle, size_t len_haystack, size_t len_needle);
    int get_content_length(const char* buf, const int size);
    int get_content_length1(const char *buf, const int size, const char *lengthname);

    std::unique_ptr<folly::IOBuf> _body;
    std::string _bodystr;
    Actions _action;    
    int _querynumber = 0;
    static std::atomic<int> _readquerycounter;
    static std::atomic<int> _writequerycounter;
    static std::atomic<int> _globalquerynumber;
  };
  
}
