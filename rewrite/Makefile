all:
#	g++ -std=c++11 -o dispatcher DispatchServer.cpp DispatchHandler.cpp -lproxygenhttpserver -lfolly -lglog -lgflags -pthread
	g++ -std=c++11 -o dispatcher DispatchServer.cpp DispatchHandler.cpp SocketPool.cpp -lproxygenhttpserver -lfolly -lglog -lgflags -pthread

debug:
	g++ -std=c++11 -g -o dispatcher DispatchServer.cpp DispatchHandler.cpp SocketPool.cpp -lproxygenhttpserver -lfolly -lglog -lgflags -pthread

clean:
	rm dispatcher
