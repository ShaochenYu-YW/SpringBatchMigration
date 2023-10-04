import csv
import time
import re
import requests
from datetime import datetime

if len(sys.argv) != 3:
    print("Usage: python adv_migration.py <input_file> <retrofill_port>")
    sys.exit(1)

input_file = sys.argv[1]
port = sys.argv[2]

# Set the endpoint URLs
adv_url = f'http://localhost:{port}/_migrate_organizations'
status_url = f'http://localhost:{port}/_check_migration_job'

def find_key_merged(response_text):
    pattern = r'id:\s*(\d+)'
    match = re.search(pattern, response_text)
    if match:
        return match.group(1)
    raise Exception(f"Cant find the job id {response_text}")


def retry_call(adv_url, batch):
    payload = {'orgIds': batch, "isTestRun": False, "splitExecution": False, "skipLdSetting": True}
    response = requests.post(adv_url, json=payload)
    if response.status_code == 200:
        response_text = response.text
        job_id = find_key_merged(response_text)
        return job_id
    else:
        return 'None'

def run_migration():
   # Read CSV file and extract IDs
   ids = []
   with open(input_file, 'r') as file:
       reader = csv.reader(file)
       header = next(reader)
       for row in reader:
           ids.append(row)

   # latest activity goes last
   ids.reverse()

   # Batch the IDs into groups of 10
   batch_size = 30
   batches = [ids[i:i + batch_size] for i in range(0, len(ids), batch_size)]
   completed_batch = 0
   start_time = datetime.now().time()
   failed_batch = []

   # Process each batch
   for batch in batches:
       # Create the payload with datasetIds
       org_ids = [data[0] for data in batch]
       print(f"Migrating these orgs: {org_ids}")
       payload = {'orgIds': org_ids, "isTestRun": False, "splitExecution": False, "skipLdSetting": True, "enableIncrementalUpdate": False}
       retry_count = 0
       max_retry = 2

       # Make a request to the endpoint and get the job ID
       response = requests.post(adv_url, json=payload)
       if response.status_code == 200:
           response_text = response.text
           print(response_text)
           job_id = find_key_merged(response_text)
           print(f'Job ID: {job_id}')

           # Check the job status periodically
           while True:
               job_status_url = f'{status_url}/{job_id}'
               job_response = requests.post(job_status_url)
               if job_response.status_code == 200:
                   response_text = job_response.text
                   first_line = response_text.split('\n')[0]
                   status = first_line.split()[-1]
                   if status == 'COMPLETED':
                       completed_batch += 1
                       print(f'Job {job_id} completed.')
                       break
                   elif status == 'FAILED':
                       if retry_count < max_retry:
                           retry_count += 1
                           job_id = retry_call(adv_url, org_ids)
                           print(f'Started a retry Job {job_id}')
                       else:
                           failed_batch.extend(org_ids)
                           break
               else:
                   print(f'Failed to find the job id {job_id}')
                   break
               # Wait for 1 minute before checking the status again
               time.sleep(10)
       else:
           print('Failed to create a job.')
           print(str(response.text))


       # Wait for a few seconds before processing the next batch
       time.sleep(2)
   end_time = datetime.now().time()


   duration = datetime.combine(datetime.today(), end_time) - datetime.combine(datetime.today(), start_time)


   # Print the duration
   print("Duration:", duration)
   print(f"completed batch size: {completed_batch}")
   print(f"failed batches: {failed_batch}")

run_migration()
