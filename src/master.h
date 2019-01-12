/*
* CS 6210 - Project 4
* Haoran Li
* GTid: 903377792
* Date: Dec.3, 2018
*/

#pragma once

#include <thread>
#include <chrono>
#include <mr_task_factory.h>
#include <grpc++/grpc++.h>

#include "file_shard.h"
#include "mapreduce_spec.h"

#include "masterworker.grpc.pb.h"

#define MAXWORKTIME 150

using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::ClientAsyncResponseReader;
using grpc::Status;
using grpc::Channel;

using masterworker::AssignTask;
using masterworker::ShardPiece;
using masterworker::PingRequest;
using masterworker::MapRequest;
using masterworker::ReduceRequest;
using masterworker::TaskReply;

using std::cout;
using std::endl;
using std::string;
using std::vector;
using std::unique_ptr;

enum work_status { MAP=0, REDUCE, IDLE, DONE, ACTIVE };
static const char* work_status_lists[] = {"MAP", "REDUCE","IDLE","DONE","ACTIVE"};


struct workerInfo {
public:
	FileShard file_shard;				// File shard which the worker is currently working on
	work_status state;					// Worker's working state
	string ip_addr;						// Work's ip address
	string job_id;						// Currently working on job's id
	string output_file_name;			// Name of output file
	int cur_work_time;					// Used to measure worker's running time
};

struct AsyncWorkerCall {
	ClientContext context;
	unique_ptr<ClientAsyncResponseReader<TaskReply>> response_reader;
	workerInfo* cur_worker;
	Status rpc_status;
	TaskReply reply;
};

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();


	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

		MapReduceSpec m_spec;
		CompletionQueue master_cq;
		vector<FileShard> shards;
		vector<workerInfo*> workers;
		vector<string> output_files;
		vector<string> jobs;							// Track all completed jobs
		vector<string> map_output_files;				// Mapper output files list
		vector<string> reduce_output_files;				// Reducer output files list
		int max_work_time;								// Max work time before assigning new task

		void assignMapTask(workerInfo* worker);
		void assignReduceTask(workerInfo* worker, int section);
		work_status pingWorkerProcess(const workerInfo* worker);
		TaskReply getSingleResponse(const string& task_type);
		bool newJob(const string& job_id);
		bool allWorkersDone(void);
};

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const vector<FileShard>& file_shards) : m_spec(mr_spec), shards(file_shards) {
	for(int cnt = 0; cnt < mr_spec.worker_ipaddr_ports.size(); cnt++) {
		string my_worker_ip = mr_spec.worker_ipaddr_ports.at(cnt);
		workerInfo* worker = new workerInfo;
		worker -> ip_addr = my_worker_ip;
		worker -> state = IDLE;
		worker -> cur_work_time = 0;
		workers.push_back(worker);
	}

	max_work_time = MAXWORKTIME;

	for (size_t cnt = 0; cnt < m_spec.num_output_files; cnt++) {
		string file_name = m_spec.output_dir + "/output_" + std::to_string(cnt);
		output_files.push_back(file_name);
	}
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {

	cout << "Start map shards!" << endl;

	bool map_complete_flg = false;
	int last_shard_assigned = 0, running_time = 200, num_shards_completed = 0, num_map_tasks_to_do = shards.size();

	// Loop until all mapping tasks be completed
	while(!map_complete_flg) {
		for(int cnt = 0; cnt < workers.size(); cnt++) {
			workerInfo* worker = workers.at(cnt);
			work_status state = pingWorkerProcess(worker);

			if(state == ACTIVE) {
				worker -> cur_work_time++;
		
				// Reassign file_shard if works more than max_work_time
				if(worker -> cur_work_time > max_work_time){
					FileShard temp_shard = worker -> file_shard;
					shards.push_back(temp_shard);
				}
			} else{
				if(worker -> state == MAP){
					FileShard dead_shard = worker -> file_shard;
					shards.push_back(dead_shard);
				}
				worker -> state = DONE;
			}
		}

		if(allWorkersDone()){
			cout << "All worker processes have already been done!" << endl;
			return false;
		}

		// Assign shards to active workers
		for(int cnt = 0; cnt < workers.size(); cnt++) {
			workerInfo* worker = workers.at(cnt);
			if(worker -> state == IDLE && shards.size() > 0) {
				FileShard assign_shard = shards.back();
				shards.pop_back();
				worker -> file_shard = assign_shard;
				worker -> state = MAP;
				assignMapTask(worker);
			}
		}

		TaskReply reply = getSingleResponse("MAP");
		if(reply.task_type().compare("MAP") == 0) {  				// If successful
			if(newJob(reply.job_id())){
				map_output_files.push_back(reply.out_file());
				num_shards_completed++;
			}
		}

		// If all shards have been assigned and completed
		if(num_shards_completed == num_map_tasks_to_do) {
			map_complete_flg = true;
			cout << "MAP PROCESS COMPLETE!" << endl;
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(running_time));
	}

	jobs.clear();
	
	cout << "Start reduce tasks!" << endl;

	bool reduce_complete_flg = false;
	int num_reduces_completed = 0, last_reduce_assigned = 0, num_reduce_tasks_to_do = output_files.size();

	// Loop until all reducing tasks be completed
	while(!reduce_complete_flg) {
		for(int cnt = 0; cnt < workers.size(); cnt++) {
			workerInfo* worker = workers.at(cnt);
			work_status state = pingWorkerProcess(worker);

			if(state == ACTIVE) {
				worker -> cur_work_time++;
				if(worker -> cur_work_time > max_work_time) {
					string temp_file = worker -> output_file_name;
					output_files.push_back(temp_file);
				}
			} else{
				worker -> state = DONE;
				if(worker -> state == REDUCE){
					string finished_file = worker -> output_file_name;
					output_files.push_back(finished_file);
				}
			}

		}
		
		if(allWorkersDone()){
			cout << "All worker processes have already been done!" << endl;
			return false;
		}

		// Assign shards to active workers
		for(int cnt = 0; cnt < workers.size(); cnt++) {
			workerInfo* worker = workers.at(cnt);
			if(worker -> state == IDLE && output_files.size() > 0) {
				string assign_file = output_files.back();
				output_files.pop_back();
				worker -> output_file_name = assign_file;
				worker -> state = REDUCE;
				last_reduce_assigned++;
				assignReduceTask(worker, cnt);
			}
		}

		TaskReply reply = getSingleResponse("REDUCE");
		if(reply.task_type().compare("REDUCE") == 0) {					// If successful
			if(newJob(reply.job_id())) {
				reduce_output_files.push_back(reply.out_file());
				num_reduces_completed++;
			}
		}

		// If all files have been assigned and processed
		if(num_reduces_completed == num_reduce_tasks_to_do){
			reduce_complete_flg = true;
		}
		
		std::this_thread::sleep_for(std::chrono::milliseconds(running_time));
	}
	return true;
}

work_status Master::pingWorkerProcess(const workerInfo* worker) {
	string worker_ip_addr = worker -> ip_addr;
	work_status res = ACTIVE;
	PingRequest request;
	auto stub = AssignTask::NewStub(grpc::CreateChannel(worker_ip_addr,grpc::InsecureChannelCredentials()));
	ClientContext context;
	TaskReply reply;
	Status status;
	
	status = stub->Ping(&context,request,&reply);

	if(!status.ok()){
		res = DONE;
		cout << "Notice! Worker " << worker_ip_addr << " is not available currently" << endl;
	}

	return res;
}

void Master::assignMapTask(workerInfo* worker){
	MapRequest request;
	auto stub = AssignTask::NewStub(grpc::CreateChannel(worker -> ip_addr, grpc::InsecureChannelCredentials()));
	request.set_user_id(m_spec.user_id);
	FileShard file_shard = worker -> file_shard;

	for (int cnt = 0; cnt < file_shard.pieces.size(); cnt++) {
		fileOffset* kv_pair = file_shard.pieces.at(cnt);
		ShardPiece* piece = request.add_shard();
		piece -> set_file_name(kv_pair -> filename);
		piece -> set_start_index(kv_pair -> start_offset);
		piece -> set_end_index(kv_pair -> end_offset);
	}

	string jobID = "map_" + std::to_string(worker -> file_shard.shard_id);
	request.set_job_id(jobID);
	request.set_num_reducers(m_spec.num_output_files);
	worker -> job_id = jobID;

	AsyncWorkerCall* call = new AsyncWorkerCall;
	call -> cur_worker = worker;
	call -> response_reader = stub -> AsyncMap(&call -> context, request, &master_cq);
	call -> response_reader -> Finish(&call -> reply, &call -> rpc_status, (void*)call);
}

void Master::assignReduceTask(workerInfo* worker, int section ) {
	ReduceRequest request;
	auto stub = AssignTask::NewStub(grpc::CreateChannel(worker->ip_addr, grpc::InsecureChannelCredentials()));
	ClientContext context;
	request.set_user_id(m_spec.user_id);
	request.set_output_file(worker -> output_file_name);
	request.set_job_id(worker -> output_file_name);
	request.set_section(std::to_string(section));

	for(auto input_file : map_output_files) {
		request.add_input_files(input_file);
	}

	worker -> job_id = worker -> output_file_name;
	
	AsyncWorkerCall* call = new AsyncWorkerCall;
	call -> cur_worker = worker;
	call -> response_reader = stub -> AsyncReduce(&call -> context, request, &master_cq);
	call -> response_reader -> Finish(&call -> reply, &call -> rpc_status, (void*)call);
}

// To determine if all workers have done their tasks
bool Master::allWorkersDone() {
	for(size_t cnt = 0; cnt < workers.size(); cnt++) {
		workerInfo* worker = workers.at(cnt);
		if(worker -> state != DONE) {
			return false;
		}
	}
	return true;
}

// To determine if a job is a new job
bool Master::newJob(const string& job_id){
	for(auto job : jobs){
		if(job.compare(job_id) == 0){
			return false;
		}
	}
	jobs.push_back(job_id);
	return true;
}

TaskReply Master::getSingleResponse(const string& task_type) {
	TaskReply reply;
	reply.set_task_type("FAIL");
	void* got_tag;
	bool ok = false;
    std::chrono::system_clock::time_point due_timestamp = std::chrono::system_clock::now() + std::chrono::milliseconds(600);
    grpc::CompletionQueue::NextStatus cq_status =  master_cq.AsyncNext(&got_tag, &ok, due_timestamp);
    if(cq_status == grpc::CompletionQueue::TIMEOUT){
        return reply;
    }
	AsyncWorkerCall* call = static_cast<AsyncWorkerCall*>(got_tag);
	workerInfo* worker = call->cur_worker;
	GPR_ASSERT(ok);

	if(call->rpc_status.ok()) {
		worker->state = IDLE;
		worker->cur_work_time = 0;
		reply = call->reply;
	} 
	/*
	else{
		cout << "ERROR!" << endl;
		cout << "Error occured in " << task_type << " with ip: " << worker -> ip_addr << endl;
		cout << call -> rpc_status.error_code() << endl;
		cout << call -> rpc_status.error_message() << endl;
	}
	*/

	return reply;
}
