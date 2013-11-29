#include "client/dfs_client.h"
#include "datanode/ext.h"

int connect_to_nn(char* address, int port)
{
	assert(address != NULL);
	assert(port >= 1 && port <= 65535);
	//TODO: create a socket and connect it to the server (address, port)
	//assign return value to client_socket 
	int client_socket = create_client_tcp_socket(address,port);
	
	return client_socket;
}

int modify_file(char *ip, int port, const char* filename, int file_size, int start_addr, int end_addr)
{
	int namenode_socket = connect_to_nn(ip, port);
	if (namenode_socket == INVALID_SOCKET) return -1;
	FILE* file = fopen(filename, "rb");
	assert(file != NULL);

	//TODO:fill the request and send
	dfs_cm_client_req_t request;
	
	//TODO: receive the response
	dfs_cm_file_res_t response;

	//TODO: send the updated block to the proper datanode

	fclose(file);
	return 0;
}

int push_file(int namenode_socket, const char* local_path)
{
	printf("local_path: %s\n",local_path);
	assert(namenode_socket != INVALID_SOCKET);
	assert(local_path != NULL);
	FILE* file = fopen(local_path, "rb");
	assert(file != NULL);

	// Create the push request
	dfs_cm_client_req_t request;

	//TODO:fill the fields in request and 
	request.req_type =1;
	memcpy(request.file_name,local_path,sizeof(request.file_name));
	fseek(file,0,SEEK_END);
	int file_size = ftell(file);
	request.file_size = file_size;
	fseek(file,0,SEEK_SET);

	send_data(namenode_socket,&request,sizeof(dfs_cm_client_req_t));

	//TODO:Receive the response
	printf("here!\n");
	dfs_cm_file_res_t response;
	receive_data(namenode_socket,&response,sizeof(dfs_cm_file_res_t));
	printf("@client push_file response.query_result.blocknum: %i\n",response.query_result.blocknum);
	//TODO: Send blocks to datanodes one by one
	dfs_cli_dn_req_t dreq;
	dreq.op_type = 1;
	int nblocks = response.query_result.blocknum;
	int datanode_socket;
	int i = 0;
	for(i = 0;i<nblocks;i++)
	{
		printf("in the loop!\n");
		datanode_socket = create_client_tcp_socket(response.query_result.block_list[i].loc_ip,response.query_result.block_list[i].loc_port);
		printf("response.query_result.block_list[i].loc_ip:%s , response.query_result.block_list[i].loc_port:%i\n",response.query_result.block_list[i].loc_ip,response.query_result.block_list[i].loc_port );
		memcpy(&dreq.block,&response.query_result.block_list[i],sizeof(dfs_cm_block_t));
		printf("here!\n");
		fread(dreq.block.content,1,DFS_BLOCK_SIZE,file);
		printf("datanode_socket:%i\n",datanode_socket);
		send_data(datanode_socket,&dreq,sizeof(dfs_cli_dn_req_t));
		printf("dreq.op_type:%i\n",dreq.op_type);
		printf("dreq : dn_id:%i block_id:%i loc_ip:%s loc_port:%i\n",dreq.block.dn_id,dreq.block.block_id,dreq.block.loc_ip,dreq.block.loc_port );
		close(datanode_socket);
		printf("end of loop!\n");
	}
	printf("here!\n");

	fclose(file);
	return 0;
}

int pull_file(int namenode_socket, const char *filename)
{
	assert(namenode_socket != INVALID_SOCKET);
	assert(filename != NULL);

	//TODO: fill the request, and send
	dfs_cm_client_req_t request;
	strcpy(request.file_name,filename);
	request.req_type = 0;
	printf("@client sending to namenode_socket:%i\n",namenode_socket);
	send_data(namenode_socket,&request,sizeof(dfs_cm_client_req_t));
	//TODO: Get the response
	dfs_cm_file_res_t response;
	receive_data(namenode_socket,&response,sizeof(dfs_cm_file_res_t));
	printf("@client receiving from namenode_socket:%i\n",namenode_socket);
	
	//TODO: Receive blocks from datanodes one by one
	int nblocks = response.query_result.blocknum;
	printf("nblocks:%i\n",nblocks);
	dfs_cm_block_t blocks[nblocks];

	int datanode_socket;
	dfs_cli_dn_req_t dreq;
	dreq.op_type = 0;
	int i;
	for(i = 0;i<nblocks;i++)
	{
		memcpy(&dreq.block,&response.query_result.block_list[i],sizeof(dfs_cm_block_t)); 
		send_data(datanode_socket,&dreq,sizeof(dfs_cli_dn_req_t));
		receive_data(datanode_socket,&blocks[i],sizeof(dfs_cm_block_t));
	}

	
	FILE *file = fopen(filename, "wb");
	//TODO: resemble the received blocks into the complete file
	for(i = 0;i < nblocks ; i++)
	{
		fwrite(blocks[i].content,1,DFS_BLOCK_SIZE,file);
	}
	fclose(file);
	return 0;
}

dfs_system_status *get_system_info(int namenode_socket)
{
	assert(namenode_socket != INVALID_SOCKET);
	//TODO fill the result and send 
	dfs_cm_client_req_t request;
	request.req_type = 2 ;
	// printf("sending request namenode_socket: %i\n",namenode_socket);
	send_data(namenode_socket,&request,sizeof(request));
	
	//TODO: get the response
	dfs_system_status *response =(dfs_system_status *) malloc(sizeof(dfs_system_status));
	// printf("receiving response\n");
	receive_data(namenode_socket,response,sizeof(dfs_system_status));

	return response;		
}

int send_file_request(char **argv, char *filename, int op_type)
{
	int namenode_socket = connect_to_nn(argv[1], atoi(argv[2]));
	printf("connect to nn\n");
	if (namenode_socket < 0)
	{
		return -1;
	}

	int result = 1;
	switch (op_type)
	{
		case 0:
			printf("pull file\n");
			result = pull_file(namenode_socket, filename);
			break;
		case 1:
			printf("push file\n");
			result = push_file(namenode_socket, filename);
			break;
	}
	close(namenode_socket);
	return result;
}

dfs_system_status *send_sysinfo_request(char **argv)
{
	int namenode_socket = connect_to_nn(argv[1], atoi(argv[2]));
	if (namenode_socket < 0)
	{
		return NULL;
	}
	dfs_system_status* ret =  get_system_info(namenode_socket);
	close(namenode_socket);
	return ret;
}
