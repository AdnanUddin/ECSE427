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
	// printf("@datanode inside the mainloop\n");
	while (safeMode == 1)
	{
		printf("the namenode is running in safe mode\n");
		sleep(5);
	}
	for (;;)
	{
		// printf("in the mainloop!\n");
		sockaddr_in client_address;
		unsigned int client_address_length = sizeof(client_address);
		int client_socket = -1;
		//TODO: accept the connection from the client and assign the return value to client_socket
		client_socket =  accept(server_socket,(sockaddr *)&client_address,&client_address_length);
		// printf("client_socket %i\n",client_socket);
		assert(client_socket != INVALID_SOCKET);
		// printf("client_socket in mainloop namenode :%i\n", client_socket);
		// printf("server_socket in mainloop namenode :%i\n", server_socket);
		dfs_cm_client_req_t request;
		//TODO: receive requests from client and fill it in request
		// printf("going to receive data\n");
		receive_data(client_socket,&request,sizeof(request));
		// printf("recieved at namenode\n");
		requests_dispatcher(client_socket, request);
		close(client_socket);
	}
	return 0;
}

static void *heartbeatService()
{
	int socket_handle = create_server_tcp_socket(50030);
	// printf("heartbeat_socket : %i \n",socket_handle );
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
	// printf("@start beg in namenode\n");
	assert(argc == 2);
	int i = 0;
	for (i = 0; i < MAX_DATANODE_NUM; i++) dnlist[i] = NULL;
	for (i = 0; i < MAX_FILE_COUNT; i++) file_images[i] = NULL;

	//TODO:create a thread to handle heartbeat service
	//you can implement the related function in dfs_common.c and call it here
	create_thread(heartbeatService,NULL);
	int server_socket = INVALID_SOCKET;
	//TODO: create a socket to listen the client requests and replace the value of server_socket with the socket's fd
	// printf("argv:%i\n", atoi(argv[1]));
	server_socket = create_server_tcp_socket(atoi(argv[1]));
	// printf("server_socket created\n");
	assert(server_socket != INVALID_SOCKET);
	// printf("@start end in namenode\n");
	return mainLoop(server_socket);
}

int register_datanode(int heartbeat_socket)
{
	for (;;)
	{
		int datanode_socket = -1;
		sockaddr_in buffer ;
		int buffer_size = sizeof(sockaddr_in);
		//TODO: accept connection from DataNodes and assign return value to datanode_socket;
		datanode_socket = accept(heartbeat_socket,(sockaddr *)&buffer, (socklen_t*) &buffer_size);
		// printf("@namenode register datanode accepted\n");
		assert(datanode_socket != INVALID_SOCKET);
		dfs_cm_datanode_status_t datanode_status;
		//TODO: receive datanode's status via datanode_socket
		// memset(&datanode_status,'0',sizeof(dfs_cm_datanode_status_t));
		// printf("receive in register datanode datanode_socket:%i\n",datanode_socket);
		receive_data(datanode_socket,&datanode_status,sizeof(dfs_cm_datanode_status_t));

		// printf("namenode recieved, datanode_status.datanode_id:%i\n",datanode_status.datanode_id);
		if (datanode_status.datanode_id < MAX_DATANODE_NUM)
		{
			int n = datanode_status.datanode_id;
			dfs_datanode_t temp;
			memset(&temp,'0',sizeof(dfs_datanode_t));
			//TODO: fill dnlist
			//principle: a datanode with id of n should be filled in dnlist[n - 1] (n is always larger than 0)
			if(dnlist[n-1] == NULL)
			{
				dncnt++;
				dnlist[n-1]= (dfs_datanode_t *)malloc(sizeof(dfs_datanode_t));
				char* ip = inet_ntoa(buffer.sin_addr);
				strcpy(temp.ip,ip);
				temp.dn_id = n;
				temp.port = ntohs(buffer.sin_port);
				dnlist[n-1] = &temp;	
				printf("datanode id %i, ip %s, port %i\n", dnlist[n-1]->dn_id, dnlist[n-1]->ip, dnlist[n-1]->port);		
			}
			
			safeMode = 0;
		}
		close(datanode_socket);
	}
	return 0;
}

int get_file_receivers(int client_socket, dfs_cm_client_req_t request)
{
	// printf("Responding to request for block assignment of file '%s'!\n", request.file_name);

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

	int datanode_index = 0;
	int block_index = first_unassigned_block_index;
	while (block_index < block_count)
	{
		memcpy((*file_image)->block_list[block_index].owner_name, (*file_image)->filename, sizeof((*file_image)->filename));
		(*file_image)->block_list[block_index].block_id = block_index;

		(*file_image)->block_list[block_index].dn_id = dnlist[datanode_index]->dn_id;
		memcpy((*file_image)->block_list[block_index].loc_ip, dnlist[datanode_index]->ip, sizeof(dnlist[datanode_index]->ip));

		(*file_image)->block_list[block_index].loc_port = dnlist[datanode_index]->port;

		block_index++;
		datanode_index++;
		if (datanode_index == dncnt) datanode_index = 0;		
	}
	//TODO: fill the response and send it back to the client
	memcpy(response.query_result.filename, (*file_image)->filename, sizeof(response.query_result.filename));
	response.query_result.blocknum = block_count;
	response.query_result.file_size = request.file_size;
	memcpy(response.query_result.block_list, (*file_image)->block_list, sizeof(response.query_result.block_list));
	send_data(client_socket,&response,sizeof(dfs_cm_file_res_t));

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
		// dfs_cm_file_t* file_images[MAX_FILE_COUNT];

		memcpy(&response.query_result,file_image,sizeof(dfs_cm_file_t));
		send_data(client_socket,&response,sizeof(dfs_cm_file_res_t));

		return 0;
	}
	//FILE NOT FOUND
	return 1;
}

void get_system_information(int client_socket, dfs_cm_client_req_t request)
{
	int i;
	assert(client_socket != INVALID_SOCKET);
	//TODO:fill the response and send back to the client
	dfs_system_status response;
	response.datanode_num = dncnt;
	for ( i = 0; i < dncnt; i++)
	{
		memcpy(&response.datanodes[i], dnlist[i], sizeof(dfs_datanode_t));
		// printf("response.datanodes[%i].dn_id : %i\n",i,response.datanodes[i].dn_id );
	}
	// memcpy(&response.datanodes,dnlist,sizeof(response.datanodes));
	send_data(client_socket,&response,sizeof(dfs_system_status));
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
