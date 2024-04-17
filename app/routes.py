from app import webserver
from flask import request, jsonify

import os
import json


# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        print(f"got data in post {data}")

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}

        # Sending back a JSON response
        return jsonify(response)
    else:
        # Method Not Allowed
        return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    print(f"JobID is {job_id}")

    # Checking if the job has been registered

    if f"job_id_{job_id}" in webserver.tasks_runner.job_status:
        # If the job has been executed, return the specific data
        if webserver.tasks_runner.job_status[f"job_id_{job_id}"] == "done":
            with open(f'./results/job_id_{job_id}.json', 'r') as f:
                data = json.load(f)
            return data
        elif webserver.tasks_runner.job_status[f"job_id_{job_id}"] == "running":
            return jsonify({'status': 'running'})
    else:
        return jsonify({'error': 'Invalid job_id'})
   
   
@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    data = request.json
    print(f"Got request {data}")

    # Extract the data required by the question
    question = data["question"]
    values = webserver.data_ingestor.check_question(question)

    # Associate all states to a list of values
    new_data = values.groupby('LocationDesc')['Data_Value'].agg(list).reset_index()

    # Send the necessary info to the thread pool
    webserver.tasks_runner.job["job_id"] = webserver.job_counter
    webserver.tasks_runner.job["data"] = new_data
    webserver.tasks_runner.job["name"] = "states_mean"
    webserver.tasks_runner.add_job()
    webserver.job_counter += 1

    return jsonify({"status": "done" ,"job_id": webserver.tasks_runner.job["job_id"]}), 200

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request(): 
    data = request.json
    print(f"Got request {data}")
    question = data["question"]
    
    # Extract only the given state's values
    values = webserver.data_ingestor.check_question(question)
    new_data = values[values['LocationDesc'] == data["state"]]
    
    webserver.tasks_runner.job["job_id"] = webserver.job_counter
    webserver.tasks_runner.job["data"] = new_data
    webserver.tasks_runner.job["name"] = "state_mean"
    webserver.tasks_runner.add_job()
    webserver.job_counter += 1
   

    return jsonify({"status": "done" ,"job_id": webserver.tasks_runner.job["job_id"]})


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    # TODO
    data = request.json
    print(f"Got request {data}")
    question = data["question"]

    # Check if the question requires the best5 max or best5 min
    if question in webserver.data_ingestor.questions_best_is_max:
        webserver.tasks_runner.job["question"] = "isMax"
    else:
        webserver.tasks_runner.job["question"] = "isMin"

    values = webserver.data_ingestor.check_question(question)
    new_data = values.groupby('LocationDesc')['Data_Value'].agg(list).reset_index()

    webserver.tasks_runner.job["job_id"] = webserver.job_counter
    webserver.tasks_runner.job["data"] = new_data
    webserver.tasks_runner.job["name"] = "best5"
    
    webserver.tasks_runner.add_job()
    webserver.job_counter += 1

    return jsonify({"status": "done" ,"job_id": webserver.tasks_runner.job["job_id"]})

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    data = request.json
    print(f"Got request {data}")
    question = data["question"]

    # Check if the question requires the worst5 max or worst5 min
    if question in webserver.data_ingestor.questions_best_is_max:
        webserver.tasks_runner.job["question"] = "isMax"
    else:
        webserver.tasks_runner.job["question"] = "isMin"

    values = webserver.data_ingestor.check_question(question)
    new_data = values.groupby('LocationDesc')['Data_Value'].agg(list).reset_index()

    webserver.tasks_runner.job["job_id"] = webserver.job_counter
    webserver.tasks_runner.job["data"] = new_data
    webserver.tasks_runner.job["name"] = "worst5"
    
    webserver.tasks_runner.add_job()
    webserver.job_counter += 1

    return jsonify({"status": "done" ,"job_id": webserver.tasks_runner.job["job_id"]})


@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    data = request.json
    print(f"Got request {data}")

    question = data["question"]
    values = webserver.data_ingestor.check_question(question)
    new_data = values.groupby('LocationDesc')['Data_Value'].agg(list).reset_index()

    webserver.tasks_runner.job["job_id"] = webserver.job_counter
    webserver.tasks_runner.job["data"] = new_data
    webserver.tasks_runner.job["name"] = "global_mean"
    webserver.tasks_runner.add_job()
    webserver.job_counter += 1
    
    return jsonify({"status": "done" ,"job_id": webserver.tasks_runner.job["job_id"]}), 200
    


@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    data = request.json
    print(f"Got request {data}")

    question = data["question"]
    values = webserver.data_ingestor.check_question(question)
    new_data = values.groupby('LocationDesc')['Data_Value'].agg(list).reset_index()
    
    webserver.tasks_runner.job["job_id"] = webserver.job_counter
    webserver.tasks_runner.job["data"] = new_data
    webserver.tasks_runner.job["name"] = "diff_from_mean"
    webserver.tasks_runner.add_job()
    webserver.job_counter += 1

    return jsonify({"status": "done" ,"job_id": webserver.tasks_runner.job["job_id"]}), 200

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    data = request.json
    print(f"Got request {data}")
    question = data["question"]
    
    new_data = webserver.data_ingestor.check_question(question)   
    webserver.tasks_runner.job["job_id"] = webserver.job_counter
    webserver.tasks_runner.job["data"] = new_data
    webserver.tasks_runner.job["name"] = "state_diff_from_mean"
    webserver.tasks_runner.job["state"]= data["state"]
    webserver.tasks_runner.add_job()
    webserver.job_counter += 1

    return jsonify({"status": "done" ,"job_id": webserver.tasks_runner.job["job_id"]})
@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    return jsonify({"status": "NotImplemented"})

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    return jsonify({"status": "NotImplemented"})

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
