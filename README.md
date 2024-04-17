# Tema 1 ASC
Vijaeac Rares - 332CD

## data_ingestor.py

In data_ingestor.py I extracted all the required data from the .csv file using the panda library: all states and their associated values according to a given question.

## task_runner.py

  I used a job queue to store all the jobs coming from the requests. Every job was a dictionary containing the following:
  - job_id - a unique id associated to every job
  - data - the required data extracted from the csv needed to process
  - name - for the thread to know which function to execute on the received data
  - state - for the requests that involved only one state
  - question - for the best5 and worst5 functions to check if the best5 and worst5 are the highest or lowest values

  For every job that a thread started to execute I updated the job_status dictionary with the status of the job and when the job was successfully executed the result would be stored in a .json file in the /results folder

## routes.py

  The structure is pretty similar for every request:
  - The function extracts the data and the question if request = POST
  - Using the data ingestor, get all the necessary info from the .csv file
  - Send a job to be added to the job queue in the form of a dictionary that included all the required data for the thread
  - Increment the job counter
  - Return the request status and the job_id associated to the job



