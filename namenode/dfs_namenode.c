#include "namenode/dfs_namenode.h"
#include <assert.h>
#include <unistd.h>

dfs_datanode_t* dnlist[MAX_DATANODE_NUM];
dfs_cm_file_t* file_images[MAX_FILE_COUNT];
int fileCount;
int dncnt;
int safeMode = 1;

int mainLoop(int server_socket)
{
	while (safeMode == 1)
	{
		printf("the namenode is running in safe mode\n");
		sleep(5);
	}

	for (;;)
	{
		sockaddr_in client_address;
		unsigned int client_address_length = sizeof(client_address);
		int client_socket = -1;
		//TODO: accept the connection from the client and assign the return value to client_socket
		assert(server_socket != INVALID_SOCKET);
		client_socket =  accept(server_socket,(struct sockaddr *)&client_address,client_address_length);
		assert(client_socket != INVALID_SOCKET);

		dfs_cm_client_req_t request;
		//TODO: receive requests from client and fill it in request
		receive_data(client_socket,&request,sizeof(request));
		requests_dispatcher(client_socket, request);
		printf("request.req_type : %i\n",request.req_type);
		send_data(client_socket,&request,sizeof(request));
		close(client_socket);
	}
	return 0;
}

static void *heartbeatService()
{
	int socket_handle = create_server_tcp_socket(50030);
	printf("heartbeat_socket : %i \n",socket_handle );
	register_datanode(socket_handle);
	close(socket_handle);
	return 0;
}


/**
 * start the service of namenode
 * argc - count of parameters
 * argv - parameters
 */
int start(int argc, char **argv)
{
	assert(argc == 2);
	int i = 0;
	for (i = 0; i < MAX_DATANODE_NUM; i++) dnlist[i] = NULL;
	for (i = 0; i < MAX_FILE_COUNT; i++) file_images[i] = NULL;

	//TODO:create a thread to handle heartbeat service
	//you can implement the related function in dfs_common.c and call it here
	create_thread(heartbeatService,NULL);
	int server_socket = INVALID_SOCKET;
	//TODO: create a socket to listen the client requests and replace the value of server_socket with the socket's fd
	printf("argv:%i\n", atoi(argv[1]));
	server_socket = create_server_tcp_socket(atoi(argv[1]));
	assert(server_socket != INVALID_SOCKET);
	return mainLoop(server_socket);
}

int register_datanode(int heartbeat_socket)
{
	for (;;)
	{
		int datanode_socket = -1;
		struct sockaddr_in buffer;
		int buffer_size = sizeof(buffer);
		//TODO: accept connection from DataNodes and assign return value to datanode_socket;
		printf("inside namenode register_datanode, before accept\n");
		printf("namenode heartbeat_socket: %i\n",heartbeat_socket);
		listen(heartbeat_socket,10);
		datanode_socket = accept(heartbeat_socket,(struct sockaddr *)&buffer,(socklen_t*)&buffer_size);
		
		assert(datanode_socket != INVALID_SOCKET);
		printf("inside namenode register_datanode, datanode socket valid!\n");
		dfs_cm_datanode_status_t datanode_status;
		//TODO: receive datanode's status via datanode_socket
		receive_data(datanode_socket,&datanode_status,sizeof(datanode_status));
		printf("received data in namenode\n");
		if (datanode_status.datanode_id < MAX_DATANODE_NUM)
		{
			//TODO: fill dnlist
			//principle: a datanode with id of n should be filled in dnlist[n - 1] (n is always larger than 0)
			printf("datanode_status.datanode_id:%i \ndatanode_status.datanode_listen_port:%i\n",datanode_status.datanode_id,datanode_status.datanode_listen_port);
			memcpy(dnlist[datanode_status.datanode_id - 1],&datanode_status,sizeof(dnlist[datanode_status.datanode_id - 1]));
			printf("filled in dnlist[%i]\n", datanode_status.datanode_id -1);
			safeMode = 0;
		}
		close(datanode_socket);
	}
	return 0;
}

int get_file_receivers(int client_socket, dfs_cm_client_req_t request)
{
	printf("Responding to request for block assignment of file '%s'!\n", request.file_name);

	dfs_cm_file_t** end_file_image = file_images + MAX_FILE_COUNT;
	dfs_cm_file_t** file_image = file_images;
	
	// Try to find if there is already an entry for that file
	while (file_image != end_file_image)
	{
		if (*file_image != NULL && strcmp((*file_image)->filename, request.file_name) == 0) break;
		++file_image;
	}

	if (file_image == end_file_image)
	{
		// There is no entry for that file, find an empty location to create one
		file_image = file_images;
		while (file_image != end_file_image)
		{
			if (*file_image == NULL) break;
			++file_image;
		}

		if (file_image == end_file_image) return 1;
		// Create the file entry
		*file_image = (dfs_cm_file_t*)malloc(sizeof(dfs_cm_file_t));
		// memset(*file_image, 0, sizeof(*file_image));
		memset(*file_image, 0, sizeof(**file_image));
		strcpy((*file_image)->filename, request.file_name);
		(*file_image)->file_size = request.file_size;
		(*file_image)->blocknum = 0;
	}
	
	int block_count = (request.file_size + (DFS_BLOCK_SIZE - 1)) / DFS_BLOCK_SIZE;
	
	int first_unassigned_block_index = (*file_image)->blocknum;
	(*file_image)->blocknum = block_count;
	int next_data_node_index = 0;

	//TODO:Assign data blocks to datanodes, round-robin style (see the Documents)

	dfs_cm_file_res_t response;
	memset(&response, 0, sizeof(response));
	//TODO: fill the response and send it back to the client

	return 0;
}

int get_file_location(int client_socket, dfs_cm_client_req_t request)
{
	int i = 0;
	for (i = 0; i < MAX_FILE_COUNT; ++i)
	{
		dfs_cm_file_t* file_image = file_images[i];
		if (file_image == NULL) continue;
		if (strcmp(file_image->filename, request.file_name) != 0) continue;
		dfs_cm_file_res_t response;
		//TODO: fill the response and send it back to the client

		return 0;
	}
	//FILE NOT FOUND
	return 1;
}

void get_system_information(int client_socket, dfs_cm_client_req_t request)
{
	assert(client_socket != INVALID_SOCKET);
	//TODO:fill the response and send back to the client
	dfs_system_status response;
	response.datanode_num = dncnt;
	memcpy(&response.datanodes,dnlist,sizeof(response.datanodes));
	send_data(client_socket,&response,sizeof(response));
}

int get_file_update_point(int client_socket, dfs_cm_client_req_t request)
{
	int i = 0;
	for (i = 0; i < MAX_FILE_COUNT; ++i)
	{
		dfs_cm_file_t* file_image = file_images[i];
		if (file_image == NULL) continue;
		if (strcmp(file_image->filename, request.file_name) != 0) continue;
		dfs_cm_file_res_t response;
		//TODO: fill the response and send it back to the client
		// Send back the data block assignments to the client
		memset(&response, 0, sizeof(response));
		//TODO: fill the response and send it back to the client
		return 0;
	}
	//FILE NOT FOUND
	return 1;
}

int requests_dispatcher(int client_socket, dfs_cm_client_req_t request)
{
	//0 - read, 1 - write, 2 - query, 3 - modify
	switch (request.req_type)
	{
		case 0:
			get_file_location(client_socket, request);
			break;
		case 1:
			get_file_receivers(client_socket, request);
			break;
		case 2:
			get_system_information(client_socket, request);
			break;
		case 3:
			get_file_update_point(client_socket, request);
			break;
	}
	return 0;
}

int main(int argc, char **argv)
{
	int i = 0;
	for (; i < MAX_DATANODE_NUM; i++)
		dnlist[i] = NULL;
	return start(argc, argv);
}
