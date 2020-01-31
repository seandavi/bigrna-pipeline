from google.cloud import pubsub

import subprocess
import logging
import traceback
import sys
import json
import os

import pkg_resources

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

subscriber = pubsub.SubscriberClient()

class Config():
    DATABASE_URL = os.environ.get('DATABASE_URL')
    PROJECT_ID = os.environ.get("PROJECT_ID")
    PUBSUB_SUBSCRIPTION = os.environ.get("PUBSUB_SUBSCRIPTION")
    PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC")

def make_command_line(experiment: str, run_id: str, index: str, gtf: str):
    nf_loc = pkg_resources.resource_filename('bigrna_pipeline', 'main.nf')
    return f'./nextflow run {nf_loc} --run_id {run_id} --experiment {experiment} -with-trace -with-report report.html --index {index} --gtf {gtf} --transcript_version v32'.split()

def callback(message):
    logging.info('handling message %s', message)
    dat = json.loads(message.data.decode('UTF-8'))
    idx = 'gencode.v32.all_transcripts.k31'
    gtf = 'gencode.v32.annotation.gtf.gz'
    z = make_command_line(dat['accession'], dat['run'], idx, gtf)
    logging.debug(f'running command "{z}"')
    try:
        proc_result = subprocess.run(z, capture_output = True, check = True)
        if(proc_result.returncode==0): #successful
            logging.info(f'successfully completed {message}')
            with open('success.txt', 'w') as f:
                f.writelines(proc_result.stdout.decode('UTF-8'))
    except:
        logging.exception(f'{message} failed with exit code {proc_result.returncode}')
        with open('failed.txt', 'w') as f:
            f.writelines(proc_result.stderr.decode('UTF-8'))
        
        
    with open('process_stdout.txt', 'w') as f:
        f.writelines(proc_result.stdout.decode('UTF-8'))
    sys.stdout.writelines(proc_result.stdout.decode('UTF-8'))
    with open('process_stderr.txt', 'w') as f:
        f.writelines(proc_result.stderr.decode('UTF-8'))
    sys.stderr.writelines(proc_result.stderr.decode('UTF-8'))
    # we are going to ack everything
    message.ack()
    logging.debug(f'generated command-line {z}')

    # cleanup directory
    logging.info('copying trace and report files')
    files_to_capture = [
        'success.txt',
        'failed.txt',
        'process_stdout.txt',
        'process_stderr.txt',
        'trace.txt',
        'report.html'
    ]
    for fname in files_to_capture:
        try:
            subprocess.run(f'gsutil -h x-goog-meta-bigrna-run:{dat["run"]} cp {fname} gs://bigrna-cancerdatasci-org/v2/{dat["run"]}/{fname}', shell=True)
        except:
            pass
    try:
        subprocess.run('rm -rf work', shell=True)
    except:
        pass
    try:
        subprocess.run('rm '+' '.join(files_to_capture), shell=True)
    except:
        pass
    
# Substitute PROJECT and SUBSCRIPTION with appropriate values for your
# application.
subscription_path = subscriber.subscription_path(Config.PROJECT_ID, Config.PUBSUB_SUBSCRIPTION)

# Open the subscription, passing the callback.
fc = pubsub.types.FlowControl(max_messages=1, max_lease_duration=7*24*60*60) #7 days
logging.info(f'awaiting messages')
future = subscriber.subscribe(subscription_path, callback, flow_control=fc)

if __name__ == '__main__':
    future.result()
