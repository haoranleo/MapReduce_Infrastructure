/*
* CS 6210 - Project 4
* Haoran Li
* GTid: 903377792
* Date: Dec.2, 2018
*/

#pragma once

#include <deque>
#include <fstream>
#include <mr_task_factory.h>

#include <grpc++/grpc++.h>

#include "mr_tasks.h"
#include "masterworker.grpc.pb.h"


using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;

using masterworker::AssignTask;
using masterworker::ShardPiece;
using masterworker::PingRequest;
using masterworker::MapRequest;
using masterworker::ReduceRequest;
using masterworker::TaskReply;

using std::cout;
using std::endl;
using std::string;
using std::ifstream;
using std::unique_ptr;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const string& user_id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

    public:
        /* DON'T change the function signature of this constructor */
        Worker(string ip_addr_port);

        /* DON'T change this function's signature */
        bool run();

        ~Worker();

        enum WorkerStatus { IDLE, MAPPING, REDUCING };

        WorkerStatus work_status;

        WorkerStatus get_status(){
            return Worker::work_status;
        }

        void set_status(WorkerStatus status){
            Worker::work_status = status;
        }

    private:
        /* NOW you can add below, data members and member functions as per the need of your implementation*/
        enum JobType { PING = 1, MAP = 2, REDUCE = 3 };
        AssignTask::AsyncService task_service;
        unique_ptr<ServerCompletionQueue> task_cq;
        unique_ptr<Server> task_server;
        ServerContext task_ctx;
        string port;

        class CallData {
            public:
                CallData(AssignTask::AsyncService* service, ServerCompletionQueue* cq, JobType job, string port)
                    : service_(service), cq_(cq), ping_responder(&ctx_), map_responder(&ctx_), reduce_responder(&ctx_), 
                    status_(CREATE), job_type(job), worker_id(port) {
                        Proceed();
                    }

                void Proceed() {
                    switch(job_type) {
                        case(PING):
                            PingProceed();
                            break;
                        case(MAP):
                            MapProceed();
                            break;
                        case(REDUCE):
                            ReduceProceed();
                            break;
                    }
                }

                void PingProceed() {
                    if (status_ == CREATE) {
                        // go to PROCESS status
                        status_ = PROCESS;
                        service_ -> RequestPing(&ctx_, &req_ping, &ping_responder, cq_, cq_, this);
                    } else if (status_ == PROCESS) {
                        // generate new CallData to handle new incoming requests
                        new CallData(service_, cq_, PING, worker_id);
                        status_ = FINISH;
                        reply_task.set_task_type("PING");
                        ping_responder.Finish(reply_task, Status::OK, this);
                    } else {
                        GPR_ASSERT(status_ == FINISH);
                        delete this;
                    }
                }

                void MapProceed() {
                    if (status_ == CREATE) {
                        // go to PROCESS status
                        status_ = PROCESS;
                        service_ -> RequestMap(&ctx_, &req_map, &map_responder, cq_, cq_, this);

                    } else if (status_ == PROCESS) {
                        // generate new CallData to handle new incoming requests
                        new CallData(service_, cq_, MAP, worker_id);
                        reply_task.set_task_type("MAP");
                        auto mapper = get_mapper_from_task_factory(req_map.user_id());
                        auto shard = req_map.shard();

                        for (size_t idx = 0; idx < shard.size(); idx++) {
                            auto shard_piece = shard.Get(idx);
                            ifstream file_shard(shard_piece.file_name());
                            file_shard.seekg(shard_piece.start_index());

                            while((file_shard.tellg() < shard_piece.end_index()) && file_shard.good())
                            {
                                string line; 
                                std::getline(file_shard, line);
                                mapper -> map(line);
                            }

                            // Write the BaseMapperInteral structure to disk
                            mapper -> impl_ -> write_data(worker_id + '_' + req_map.job_id(), req_map.num_reducers());
                        }

                        // change status to FINISH
                        status_ = FINISH;
                        reply_task.set_task_type("MAP");
                        reply_task.set_job_id(req_map.job_id());
                        reply_task.set_out_file(worker_id + '_' + req_map.job_id());
                        map_responder.Finish(reply_task, Status::OK, this);

                    } else {
                        GPR_ASSERT(status_ == FINISH);
                        delete this;
                    }
                }

                void ReduceProceed() {
                    if (status_ == CREATE) {
                        // go to PROCESS status
                        status_ = PROCESS;
                        service_ -> RequestReduce(&ctx_, &req_reduce, &reduce_responder, cq_, cq_, this);

                    } else if (status_ == PROCESS) {
                        // generate new CallData to handle new incoming requests
                        new CallData(service_, cq_, REDUCE, worker_id);
                        auto reducer = get_reducer_from_task_factory(req_reduce.user_id());
                        reducer -> impl_ -> final_file = req_reduce.output_file();

                        for (auto idx : req_reduce.input_files()) {
                            // combine the reducer number and the filename
                            string combined_name = idx + "_R" + req_reduce.section();
                            reducer -> impl_ -> interm_files.push_back(combined_name);
                        }

                        // gather keys from intermediate files
                        reducer -> impl_ -> group_keys();
                        for(auto idx : reducer->impl_->pairs){
                            reducer -> reduce(idx.first, idx.second );
                        }

                        status_ = FINISH;
                        reply_task.set_task_type("REDUCE");
                        reply_task.set_job_id(req_reduce.job_id());
                        reply_task.set_out_file(req_reduce.output_file());
                        reduce_responder.Finish(reply_task, Status::OK, this);

                    } else {
                        GPR_ASSERT(status_ == FINISH);
                        delete this;
                    }
                }


            private:
                AssignTask::AsyncService* service_;
                ServerCompletionQueue* cq_;                                 // The producer-consumer queue used in asynchronous call
                ServerContext ctx_;

                // Request & reply, to & from client
                PingRequest req_ping;
                MapRequest req_map;
                ReduceRequest req_reduce;
                TaskReply reply_task;

                // Job & worker information
                JobType job_type;
                string worker_id;

                // The means to get back to the client.
                ServerAsyncResponseWriter<TaskReply> ping_responder;
                ServerAsyncResponseWriter<TaskReply> map_responder;
                ServerAsyncResponseWriter<TaskReply> reduce_responder;

                enum CallStatus { CREATE, PROCESS, FINISH };
                CallStatus status_;                                         // Current serving status.
        };
};

Worker::~Worker(){
    task_server->Shutdown();
    task_cq->Shutdown();
}


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(string ip_addr_port): work_status(IDLE) {
    ServerBuilder builder;
    builder.AddListeningPort(ip_addr_port, grpc::InsecureServerCredentials());
    builder.RegisterService(&task_service);
    task_cq = builder.AddCompletionQueue();
    task_server = builder.BuildAndStart();
    port = ip_addr_port.substr((ip_addr_port.find(':') + 1), ip_addr_port.length());
}


/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and
	BaseReduer's member BaseReducerInternal impl_ directly,
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
    void* tag;
    bool ok;

    new CallData(&task_service, task_cq.get(),PING, port);              // Listen for ping request
    new CallData(&task_service, task_cq.get(),MAP, port);               // Listen for map request
    new CallData(&task_service, task_cq.get(),REDUCE, port);            // Listen for reduce request

    while(true) {
        GPR_ASSERT(task_cq->Next(&tag,&ok));
        GPR_ASSERT(ok);
        static_cast<CallData*>(tag)->Proceed();
    }
}
