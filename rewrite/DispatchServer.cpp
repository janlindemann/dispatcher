#include "DispatchHandler.h"
#include "proxygen/httpserver/HTTPServer.h"
#include "proxygen/httpserver/RequestHandlerFactory.h"
#include "Settings.h"

#include <folly/Portability.h>
#include <folly/Memory.h>
#include <folly/io/async/EventBaseManager.h>

#include <algorithm>
#include <arpa/inet.h>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

using namespace DispatchService;
using namespace proxygen;

using folly::EventBase;
using folly::EventBaseManager;
using folly::SocketAddress;

using Protocol = HTTPServer::Protocol;

// command line flags parsed by gflags::ParseCommandLineFlags
DEFINE_int32(http_port, 11000, "Port to listen on with HTTP protocol");
DEFINE_int32(thrift_port, 10000, "Port to listen on for thrift");
DEFINE_string(ip, "0.0.0.0", "IP/Hostname to bind to");
DEFINE_int32(threads, 10, "Number of threads to listen on.");
#define MAX_SOCKETS 500

class DispatchHandlerFactory : public RequestHandlerFactory {
public:
  void onServerStart() noexcept override {}
  void onServerStop() noexcept override {}

  RequestHandler* onRequest(RequestHandler*, HTTPMessage*) noexcept override {
    return new DispatchHandler();
  }
};


void usage() {
  std::cout << "Usage:\n\t./dispatcher port host1:port1 [hostx:portx ...]" << std::endl;
  exit(1);
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (argc<3)
    usage();

  int port = std::atoi(argv[1]);

  // read hosts from command line arguments int DispatchService::hosts
  int numberhosts = argc - 2;
  Settings::get_instance()->number_of_hosts = numberhosts;
  Settings::get_instance()->hosts = new std::string*[numberhosts];
  for(int i=0; i<numberhosts; ++i) {
    int arg_len = strlen(argv[i+2]);
    int colon = std::find(argv[i+2], argv[i+2] + arg_len, ':') - argv[i+2];
    Settings::get_instance()->hosts[i] = new std::string[2];
    Settings::get_instance()->hosts[i][0] = std::string(argv[i+2], 0, colon);
    Settings::get_instance()->hosts[i][1] = std::string(argv[i+2]+colon+1, arg_len-colon);
  }


  std::vector<HTTPServer::IPConfig> IPs = {
    {SocketAddress(FLAGS_ip, port, true), Protocol::HTTP},
  };

  if (FLAGS_threads <= 0) {
    FLAGS_threads = sysconf(_SC_NPROCESSORS_ONLN);
    CHECK(FLAGS_threads > 0);
  }

  HTTPServerOptions options;
  options.threads = static_cast<size_t>(FLAGS_threads);
  options.idleTimeout = std::chrono::milliseconds(60000);
  options.shutdownOn = {SIGINT, SIGTERM};
  options.handlerFactories = RequestHandlerChain()
    .addThen<DispatchHandlerFactory>()
    .build();

  HTTPServer server(std::move(options));
  server.bind(IPs);

  std::thread t([&] () {
      server.start();
    });

  t.join();
  return 0;
}

