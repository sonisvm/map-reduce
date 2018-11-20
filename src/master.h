#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"


/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
/*		std::vector<std::unique_ptr<worker::Stub>> worker_stubs;
		std::vector<std::shared_ptr<Channel>> worker_channels;
		std::map<FileShard, int> map_status; // 0 for not assigned, 1 for in-progress, 2 for done, -1 for failure
		std::map<std::string, int> reduce_status; // structure to keep track of the intermediate files and their status*/
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
	//create channels
/*	std::string worker_server;
	for(auto entry : mr_spec.worker_ipaddr_ports){
		worker_server = entry;
		//form a channel and create a Stub
		std::shared_ptr<Channel> channel = grpc::CreateChannel(worker_server, grpc::InsecureChannelCredentials());
		std::unique_ptr<Worker::Stub> worker_stub = Worker::NewStub(channel);
		worker_stubs.push_back(std::move(worker_stub));  //unique_ptr cannot be copied, have to be moved
		worker_channels.push_back(channel);
	}
	//create supporting data structures

	// structure to keep track of which shards are done
	for (auto entry : file_shards) {
		map_status[entry] = 0;
	}
*/


}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	//loop through shards and assign one to each worker
	// grpc call to each worker
	// wait till a worker responds
	// Keep assigning shards until done
	// store the intermediate file locations returned by workers
	// start assigning reduce tasks
	// wait for workers to return
	return true;
}
