from queue import Queue
from threading import Thread, Event, Semaphore
import time
import os
import multiprocessing
import json

class ThreadPool:
    def __init__(self):
        # Initate the data structures
        TP_NUM_OF_THREADS = int(os.getenv('TP_NUM_OF_THREADS', multiprocessing.cpu_count()))
        self.threads = []
        self.jobqueue = Queue()
        self.graceful_shutdown = Event()
        self.job_status = {}
        self.job = {}

        for i in range(0, TP_NUM_OF_THREADS):
            self.threads.append(TaskRunner(self.jobqueue, self.graceful_shutdown, self.job_status))

        for x in self.threads:
            x.start()
        
        # Add job in job queue
    def add_job(self):
        self.jobqueue.put(self.job)

        # If the graceful shutdown request is made, wait for the other jobs to execute
    def wait_completion(self):
        self.jobqueue.join()
        self.graceful_shutdown.set()
        for thread in self.threads:
            thread.join()

class TaskRunner(Thread):
    def __init__(self, jobqueue, graceful_shutdown, job_status):
        super(TaskRunner, self).__init__()
        self.jobqueue = jobqueue
        self.graceful_shutdown = graceful_shutdown
        self.job_status = job_status

        # Separate functions for each request

    def states_mean(self, data):
        result_list = {}
        for index, row in data.iterrows():
            temp_data = row['Data_Value']
            sum = 0
            for x in temp_data:
                sum += x
            result = sum / len(temp_data)
            result_list[row['LocationDesc']] = result
        result_list = dict(sorted(result_list.items(), key=lambda item: item[1]))
        return result_list
    
    def state_mean(self, data):
        new_data = data.groupby('LocationDesc')['Data_Value'].agg(list).reset_index()
        value_list = new_data['Data_Value'].iloc[0]
        sum = 0
        for x in value_list:
            sum += x
        return {str(new_data['LocationDesc'].iloc[0]): sum / len(value_list)}
    
    def best5(self, data, question):
        result = self.states_mean(data)
        if question == "isMax":
            return dict(list(result.items())[-5:])
        else:
            return dict(list(result.items())[:5])
        
    def worst5(self, data, question):
        result = self.states_mean(data)
        if question == "isMax":
            return dict(list(result.items())[:5])
        else:
            return dict(list(result.items())[-5:])
    
    def global_mean(self, data):
        sum = 0
        length = 0
        for index, row in data.iterrows():
            temp_data = row['Data_Value']
            for x in temp_data:
                sum += x
            length += len(temp_data)
        return {"global_mean": sum / length}
        
    def diff_from_mean(self, data):
        states_mean = self.states_mean(data)
        global_mean = self.global_mean(data)["global_mean"]
        for x in states_mean.keys():
            states_mean[x] = global_mean - states_mean[x]

        return states_mean
    
    def state_diff_from_mean(self, data, state):
        state_data = data[data['LocationDesc'] == state]
        global_data = data.groupby('LocationDesc')['Data_Value'].agg(list).reset_index()
        global_mean = self.global_mean(global_data)["global_mean"]
        state_mean = list(self.state_mean(state_data).values())[0]
        state = list(self.state_mean(state_data).keys())[0]
       
        return {state: global_mean - state_mean}



    def run(self):
        while True:
            # Extract the job from the queue

            job = self.jobqueue.get()
            if job is not None:
                job_id = job["job_id"]
                # Set the job status to running
                self.job_status[f"job_id_{job_id}"] = "running"

                if job["name"] == "states_mean":
                    result = self.states_mean(job["data"])
                    self.job_status[f"job_id_{job_id}"] = "done"

                elif job["name"] == "state_mean":
                    result = self.state_mean(job["data"])
                    self.job_status[f"job_id_{job_id}"] = "done"

                elif job["name"] == "best5":
                    result = self.best5(job["data"], job["question"])
                    self.job_status[f"job_id_{job_id}"] = "done"

                elif job["name"] == "worst5":
                    result = self.worst5(job["data"], job["question"])
                    self.job_status[f"job_id_{job_id}"] = "done"

                elif job["name"] == "global_mean":
                    result = self.global_mean(job["data"])
                    self.job_status[f"job_id_{job_id}"] = "done"
                
                elif job["name"] == "diff_from_mean":
                    result = self.diff_from_mean(job["data"])
                    self.job_status[f"job_id_{job_id}"] = "done"

                elif job["name"] == "state_diff_from_mean":
                    result = self.state_diff_from_mean(job["data"], job["state"])
                    self.job_status[f"job_id_{job_id}"] = "done"
                    
                # Create the file associated with the job
                result_dict = {"status": self.job_status[f"job_id_{job_id}"] ,"data": result}
                with open(f'./results/job_id_{job_id}.json', 'w') as f:
                    json.dump(result_dict, f)
                
                self.jobqueue.task_done()
                