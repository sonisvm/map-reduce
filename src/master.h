#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"
#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"
#include <grpcpp/grpcpp.h>
#include <chrono>
#include <thread>
#include <mutex>
#include <unistd.h>
#include <sys/stat.h>

using masterworker::TaskRequest;
using masterworker::Worker;
using masterworker::TaskResponse;
using masterworker::FilePath;

using grpc::Channel;
using grpc::CompletionQueue;
using grpc::ClientContext;
using grpc::ClientAsyncResponseReader;
using grpc::Status;
using grpc::StatusCode;

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
		std::vector<std::unique_ptr<Worker::Stub>> worker_stubs;
		std::vector<std::shared_ptr<Channel>> worker_channels;
		std::vector<FileShard> file_shards;
		std::vector<int> map_status; // 0 for not assigned, 1 for in-progress, 2 for done, -1 for failure
		std::vector<int> reduce_status; // structure to keep track of the intermediate files and their status*/
		std::map<int, int> worker_to_shard_map;
		std::map<int, int> worker_to_reduce_map;
		std::vector<int> worker_status;		// we give 3 retries to every worker. If worker fails for all, worker is not longer used
		std::vector<std::string> intermediate_files;
		MapReduceSpec mr_spec;

		std::vector<std::thread> threads;
		std::mutex task_lock;
		std::mutex intermediate_file_lock;

		void startMapperThreads();
		void waitForThreads();
		void mapPhase(int worker);
		void startReducerThreads();
		void reducePhase(int worker);

};

void Master::mapPhase(int worker){
	while (1) {
		int shard=-1;
		bool all_done=true;
		task_lock.lock();
		for (size_t i = 0; i < map_status.size(); i++) {
			if(map_status[i]==0){
				shard=i;
				map_status[i] = 1; // so that no other thread can take this
				all_done=false;
				break;
			}
			if(map_status[i]==1){
				all_done = false; // we need to wait till all shards are done, in case there are failures
			}
		}
		task_lock.unlock();

		if(shard==-1 && !all_done){
			//since there are no unassigned tasks, just yield
			std::this_thread::yield();
		} else if(all_done){
			break; //all maps are complete
		} else if(shard!=-1){
			Status status;
			TaskResponse response;
			CompletionQueue worker_response_cq;
			ClientContext client_context;

			std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
			client_context.set_deadline(deadline);

			TaskRequest request;
			//each shard can have multiple names
			for (auto i=0; i<file_shards[shard].filenames.size(); i++) {
				FilePath* file = request.add_file_paths();
				file->set_file_path(file_shards[shard].filenames[i]);
				file->set_start_offset(file_shards[shard].from_offset[i]);
				file->set_end_offset(file_shards[shard].to_offset[i]);
			}
			request.set_task_type("MAP");
			//create output directory
			std::string output_dir = mr_spec.output_dir+"/worker"+std::to_string(worker);
			mkdir(output_dir.c_str(), S_IRUSR | S_IWUSR);
			request.set_output_dir(output_dir);
			request.set_num_reducers(mr_spec.num_output_files);

			std::unique_ptr<ClientAsyncResponseReader<TaskResponse>> response_reader(worker_stubs[worker]->PrepareAsyncassignTask(&client_context, request, &worker_response_cq));

			response_reader->StartCall();

			worker_to_shard_map[worker] = shard;

			response_reader->Finish(&response, &status, (void *)worker);

			void* response_tag = (void *) worker;
			bool ok = false;

			GPR_ASSERT(worker_response_cq.Next(&response_tag, &ok));
			GPR_ASSERT(ok);

			if (status.error_code() == StatusCode::DEADLINE_EXCEEDED) {
				worker_status[worker]--;
				if(worker_status[worker]==0){
					break;  // worker is down
				}
				task_lock.lock();
				map_status[shard] = 0;
				task_lock.unlock();
			} else {
				if (status.ok()) {
					if (response.status()==1) {
						// extracting the intermediate files from response
						int result_size = response.file_paths_size();
						for (size_t i = 0; i < result_size; i++) {
							intermediate_file_lock.lock();
							//TO FIX: Make intermediate file a map between partition and filenames
							intermediate_files.push_back(response.file_paths(i).file_path());
							intermediate_file_lock.unlock();
						}
						task_lock.lock();
						map_status[shard] = 2;
						task_lock.unlock();
					} else {
						//if status is not 1, reset the task
						task_lock.lock();
						map_status[shard] = 0;
						task_lock.unlock();
					}

				} else {
					//TO DO: handle failure
					task_lock.lock();
					map_status[shard] = 0;
					task_lock.unlock();
				}
			}
		} else {
			std::cout << "Should not come here. Error!\n";
		}

	}

}

void Master::reducePhase(int worker){
	while (1) {
		int partition=-1;
		bool all_done=true;
		task_lock.lock();
		for (size_t i = 0; i < reduce_status.size(); i++) {
			if(reduce_status[i]==0){
				partition=i;
				reduce_status[i] = 1; // so that no other thread can take this
				all_done=false;
				break;
			}
			if(reduce_status[i]==1){
				all_done = false; // we need to wait till all shards are done, in case there are failures
			}
		}
		task_lock.unlock();
		if(partition==-1 && !all_done){
			//since there are no unassigned tasks, just yield
			std::this_thread::yield();
		} else if(all_done){
			break; //all reduce tasks are complete
		} else if(partition!=-1){
			ClientContext* client_context = new ClientContext();
			std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
			client_context->set_deadline(deadline);
			TaskRequest request;
			CompletionQueue worker_response_cq;
			FilePath* file = request.add_file_paths();
			file->set_file_path(intermediate_files[partition]);

			request.set_task_type("REDUCE");
			request.set_output_dir(mr_spec.output_dir);
			request.set_input_dir(mr_spec.output_dir);

			Status status;

			TaskResponse response;

			std::unique_ptr<ClientAsyncResponseReader<TaskResponse>> response_reader(worker_stubs[worker]->PrepareAsyncassignTask(client_context, request, &worker_response_cq));
			response_reader->StartCall();
			worker_to_reduce_map[worker] = partition;

			response_reader->Finish(&response, &status, (void *)worker);

			void* response_tag = (void *) worker;
			bool ok = false;
			GPR_ASSERT(worker_response_cq.Next(&response_tag, &ok));
			GPR_ASSERT(ok);
			if (status.error_code() == StatusCode::DEADLINE_EXCEEDED) {
				worker_status[worker]--;
				if(worker_status[worker]==0){
					break;  // worker is down
				}
				task_lock.lock();
				reduce_status[partition] = 0;
				task_lock.unlock();
			} else {
				if (status.ok()) {
					if (response.status()==1) {
						task_lock.lock();
						reduce_status[partition] = 2;
						task_lock.unlock();
					} else {
						//if status is not 1, reset the task
						task_lock.lock();
						reduce_status[partition] = 0;
						task_lock.unlock();
					}


				} else {
					//TO DO: handle failure
					task_lock.lock();
					reduce_status[partition] = 0;
					task_lock.unlock();
				}

			}
		} else {
			std::cout << "Should not come here. Error!\n";
		}

	}

}

void Master::startMapperThreads(){
	for (auto i = 0; i < mr_spec.num_workers; i++) {
    std::thread t(&Master::mapPhase,this,i);
    threads.push_back(std::move(t));
  }
}
void Master::startReducerThreads(){
	for (auto i = 0; i < mr_spec.num_workers; i++) {
    std::thread t(&Master::reducePhase,this,i);
    threads.push_back(std::move(t));
  }
}
void Master::waitForThreads(){
	for (auto i = 0; i < mr_spec.num_workers; i++) {
		threads[i].join();
	}
	threads.clear();
}

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {

	//create channels
	std::string worker_server;
	for(auto entry : mr_spec.worker_ipaddr_ports){
		worker_server = entry;
		//form a channel and create a Stub
		std::shared_ptr<Channel> channel = grpc::CreateChannel(worker_server, grpc::InsecureChannelCredentials());
		std::unique_ptr<Worker::Stub> worker_stub = Worker::NewStub(channel);
		worker_stubs.push_back(std::move(worker_stub));  //unique_ptr cannot be copied, have to be moved
		worker_channels.push_back(channel);
		worker_status.push_back(3);
	}


	// structure to keep track of which shards are done
	for (auto entry: file_shards) {
		this->file_shards.push_back(entry);
		this->map_status.push_back(0);
	}

	for (size_t i = 0; i < mr_spec.num_output_files; i++) {
		reduce_status.push_back(0);
	}
	this->mr_spec = mr_spec;
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	startMapperThreads();
	waitForThreads();
	//reset the worker status to 3
	startReducerThreads();
	waitForThreads();
	// CompletionQueue worker_response_cq;
	//
	// //setting up client context, status, responses
	// //ClientContext cannot be reused across rpcs
	// std::vector<ClientContext*> client_contexts;
	//
	// //we need a different status for each call
	 // std::vector<Status> statuses;
	 // std::vector<TaskResponse> responses;
	 // std::vector<std::unique_ptr<ClientAsyncResponseReader<TaskResponse>>> response_readers;
	// //loop through shards and assign one to each worker
	// int shards_initiated=0;
	// for (auto j=0; j < mr_spec.num_workers; j++) {
	// 	if (worker_status[j]>0 && map_status[shards_initiated] == 0) {
	// 		// grpc call to each worker
	// 		// all workers would be free
	// 		ClientContext* client_context = new ClientContext();
	//
	// 		std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
	// 		client_context->set_deadline(deadline);
	// 		//client_contexts.push_back(client_context);
	//
	// 		TaskRequest request;
	// 		//each shard can have multiple names
	// 		for (auto i=0; i<file_shards[shards_initiated].filenames.size(); i++) {
	// 			FilePath* file = request.add_file_paths();
	// 			file->set_file_path(file_shards[shards_initiated].filenames[i]);
	// 			file->set_start_offset(file_shards[shards_initiated].from_offset[i]);
	// 			file->set_end_offset(file_shards[shards_initiated].to_offset[i]);
	// 		}
	// 		request.set_task_type("MAP");
	// 		request.set_output_dir(mr_spec.output_dir);
	// 		request.set_num_reducers(mr_spec.num_output_files);
	//
	// 		Status status;
	// 		statuses.push_back(status);
	//
	// 		TaskResponse response;
	// 		responses.push_back(response);
	//
	// 		response_readers.push_back(std::move(worker_stubs[j]->PrepareAsyncassignTask(client_context, request, &worker_response_cq)));
	// 		response_readers[j]->StartCall();
	// 		map_status[shards_initiated]=1; //updating status to in-progress
	// 		worker_to_shard_map[j] = shards_initiated;
	// 		shards_initiated++;
	// 	}
	// }
	//int worker_response_rcvd=0;
	// while(worker_response_rcvd < mr_spec.num_workers){
	// 	// wait till a worker responds
	// 	response_readers[worker_response_rcvd]->Finish(&responses[worker_response_rcvd], &statuses[worker_response_rcvd], (void *)worker_response_rcvd);
	//
	// 	void* response_tag = (void *) worker_response_rcvd;
	// 	bool ok = false;
	// 	std::cout <<"Waiting for response\n";
	// 	GPR_ASSERT(worker_response_cq.Next(&response_tag, &ok));
	// 	GPR_ASSERT(ok);
	// 	std::cout <<"Got a response" << statuses[worker_response_rcvd].error_code() << "\n";
	// 	if (statuses[worker_response_rcvd].error_code() == StatusCode::DEADLINE_EXCEEDED) {
	// 		worker_status[worker_response_rcvd]--;
	// 		map_status[worker_to_shard_map[worker_response_rcvd]] = 0;
	// 	} else {
	// 		if (statuses[worker_response_rcvd].ok()) {
	//
	// 			cout << "Received" << "\n";
	// 			if (responses[worker_response_rcvd].status()==1) {
	// 				// extracting the intermediate files from response
	// 				int result_size = responses[worker_response_rcvd].file_paths_size();
	// 				cout << "Intermediate files stored at: \n";
	// 				for (size_t i = 0; i < result_size; i++) {
	// 					cout << responses[worker_response_rcvd].file_paths(i).file_path() << "\n";
	// 					intermediate_files.push_back(responses[worker_response_rcvd].file_paths(i).file_path());
	// 				}
	// 				map_status[worker_to_shard_map[worker_response_rcvd]] = 2;
	// 			} else {
	// 				//if status is not 1, reset the task
	// 				map_status[worker_to_shard_map[worker_response_rcvd]] = 0;
	// 			}
	//
	// 		} else {
	// 			//TO DO: handle failure
	// 			map_status[worker_to_shard_map[worker_response_rcvd]] = 0;
	// 		}
	// 	}
	// 	int i=0;
	// 	for (; i < map_status.size(); i++) {
	// 		if (map_status[i]==0 && worker_status[worker_response_rcvd]>0) {
	// 			ClientContext* client_context = new ClientContext();
	// 			std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
	// 			client_context->set_deadline(deadline);
	// 			TaskRequest request;
	// 			//each shard can have multiple names
	// 			for (auto j=0; j<file_shards[i].filenames.size(); j++) {
	// 				FilePath* file = request.add_file_paths();
	// 				file->set_file_path(file_shards[i].filenames[j]);
	// 				file->set_start_offset(file_shards[i].from_offset[j]);
	// 				file->set_end_offset(file_shards[i].to_offset[j]);
	// 			}
	// 			request.set_task_type("MAP");
	// 			request.set_output_dir(mr_spec.output_dir);
	// 			request.set_num_reducers(mr_spec.num_output_files);
	//
	// 			Status status;
	// 			statuses[worker_response_rcvd] = status; //the same worker is being used again
	//
	// 			TaskResponse response;
	// 			responses[worker_response_rcvd] = response;
	//
	// 			response_readers[worker_response_rcvd]=std::move(worker_stubs[worker_response_rcvd]->PrepareAsyncassignTask(client_context, request, &worker_response_cq));
	// 			response_readers[worker_response_rcvd]->StartCall();
	// 			map_status[i]=1; //updating status to in-progress
	// 			worker_to_shard_map[worker_response_rcvd] = i;
	// 			break;
	// 		}
	// 	}
	// 	if (i == map_status.size()) {
	// 		// there are no more shards to process
	// 		worker_response_rcvd++;
	// 	}
	//
	//
	//
	//
	// }

	// END of Map phase

	// start assigning reduce tasks
	// int reduce_task_initiated=0;
	// for (auto j=0; j < mr_spec.num_workers; j++) {
	// 	if (reduce_status[reduce_task_initiated] == 0) {
	// 		// grpc call to each worker
	// 		// all workers would be free
	// 		ClientContext* client_context = new ClientContext();
	// 		//client_contexts.push_back(client_context);
	// 		TaskRequest request;
	// 		//each shard can have multiple names
	// 		FilePath* file = request.add_file_paths();
	// 		file->set_file_path(intermediate_files[reduce_task_initiated]);
	//
	// 		request.set_task_type("REDUCE");
	// 		request.set_output_dir(mr_spec.output_dir);
	// 		request.set_input_dir(mr_spec.output_dir);
	//
	// 		Status status;
	// 		statuses[j]=status;
	//
	// 		TaskResponse response;
	// 		responses[j] = response;
	//
	// 		response_readers[j] = std::move(worker_stubs[j]->PrepareAsyncassignTask(client_context, request, &worker_response_cq));
	// 		response_readers[j]->StartCall();
	// 		reduce_status[reduce_task_initiated]=1; //updating status to in-progress
	// 		worker_to_reduce_map[j] = reduce_task_initiated;
	// 		reduce_task_initiated++;
	//
	// 	}
	// }
	//
 	// worker_response_rcvd=0;
	// while(worker_response_rcvd < mr_spec.num_workers){
	// 	// wait till a worker responds
	// 	response_readers[worker_response_rcvd]->Finish(&responses[worker_response_rcvd], &statuses[worker_response_rcvd], (void *)worker_response_rcvd);
	//
	// 	void* response_tag = (void *) worker_response_rcvd;
	// 	bool ok = false;
	//
	// 	GPR_ASSERT(worker_response_cq.Next(&response_tag, &ok));
	// 	GPR_ASSERT(ok);
	//
	// 	if (statuses[worker_response_rcvd].ok()) {
	// 		if (responses[worker_response_rcvd].status()==1) {
	// 			// extracting the intermediate files from response
	// 			map_status[worker_to_shard_map[worker_response_rcvd]] = 2;
	// 		} else {
	// 			//if status is not 1, reset the task
	// 			map_status[worker_to_shard_map[worker_response_rcvd]] = 0;
	// 		}
	//
	// 		int i=0;
	// 		for (; i < reduce_status.size(); i++) {
	// 			if (reduce_status[i]==0) {
	// 				ClientContext* client_context = new ClientContext();
	//
	// 				TaskRequest request;
	// 				//each shard can have multiple names
	// 				FilePath* file = request.add_file_paths();
	// 				file->set_file_path(intermediate_files[i]);
	//
	// 				request.set_task_type("REDUCE");
	// 				request.set_output_dir(mr_spec.output_dir);
	// 				request.set_input_dir(mr_spec.output_dir);
	//
	// 				Status status;
	// 				statuses[worker_response_rcvd] = status; //the same worker is being used again
	//
	// 				TaskResponse response;
	// 				responses[worker_response_rcvd] = response;
	//
	// 				response_readers[worker_response_rcvd] = std::move(worker_stubs[worker_response_rcvd]->PrepareAsyncassignTask(client_context, request, &worker_response_cq));
	// 				response_readers[worker_response_rcvd]->StartCall();
	// 				reduce_status[i]=1; //updating status to in-progress
	// 				worker_to_reduce_map[worker_response_rcvd] = i;
	// 				break;
	// 			}
	// 		}
	// 		if (i == reduce_status.size()) {
	// 			// there are no more shards to process
	// 			worker_response_rcvd++;
	// 		}
	//
	// 	} else {
	// 		//TO DO: handle failure
	// 		reduce_status[worker_to_reduce_map[worker_response_rcvd]] = 0;
	// 	}
	//
	// }

	// wait for workers to return
	return true;
}
