#include "common/dfs_common.h"
#include <pthread.h>
/**
 * create a thread and activate it
 * entry_point - the function exeucted by the thread
 * args - argument of the function
 * return the handler of the thread
 */
inline pthread_t * create_thread(void * (*entry_point)(void*), void *args)
{
	//TODO: create the thread and run it
	pthread_t * thread = (pthread_t *)malloc(sizeof(thread));
	pthread_create(thread,NULL,entry_point,args);

	return thread;
}

/**
 * create a socket and return
 */
int create_tcp_socket()
{
	//TODO:create the socket and return the file descriptor 
	int sd = socket(AF_INET,SOCK_STREAM,0);
	return sd;
}

/**
 * create the socket and connect it to the destination address
 * return the socket fd
 */
int create_client_tcp_socket(char* address, int port)
{
	assert(port >= 0 && port < 65536);
	int socket = create_tcp_socket();
	if (socket == INVALID_SOCKET) return 1;
	//TODO: connect it to the destination port
	struct sockaddr_in serv_addr;
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(port);
	serv_addr.sin_addr.s_addr = inet_addr(address);

	connect(socket,(struct sockaddr *)&serv_addr,sizeof(serv_addr));
	return socket;
}

/**
 * create a socket listening on the certain local port and return
 */
int create_server_tcp_socket(int port)
{
	assert(port >= 0 && port < 65536);
	int socket = create_tcp_socket();
	if (socket == INVALID_SOCKET) return 1;
	//TODO: listen on local port
	struct sockaddr_in serv_addr;
	memset(&serv_addr,'0',sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	serv_addr.sin_port = htons(port);

	int error = bind(socket,(struct sockaddr*)&serv_addr,sizeof(serv_addr));
	if(error == -1) printf("error on bind\n");
	if(listen(socket,10) == -1) {
		printf("create_server_tcp_socket() fail listen\n");
		return -1;
	}
	return socket;
}

/**
 * socket - connecting socket
 * data - the buffer containing the data
 * size - the size of buffer, in byte
 */
void send_data(int socket, void* data, int size)
{
	int data_size;
	assert(data != NULL);
	assert(size >= 0);
	if (socket == INVALID_SOCKET) return;
	//TODO: send data through socket
	data_size = send(socket,data,size,0);
	printf("data sent: %i \n",data_size);
}

/**
 * receive data via socket
 * socket - the connecting socket
 * data - the buffer to store the data
 * size - the size of buffer in byte
 */
void receive_data(int socket, void* data, int size)
{
	assert(data != NULL);
	assert(size >= 0);
	if (socket == INVALID_SOCKET) return;
	//TODO: fetch data via socket
	printf("before recv \n");
	int data_received = recv(socket,data,size,0);
	printf("data received: %i\n",data_received );
}
