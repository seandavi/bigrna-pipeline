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
    print(message)  # Replace this with your actual logic.
    dat = json.loads(message.data.decode('UTF-8'))
    idx = 'gencode.v32.all_transcripts.k31'
    gtf = 'gencode.v32.annotation.gtf.gz'
    z = make_command_line(dat['accession'], dat['run'], idx, gtf)
    logging.info(f'got new message {message}')
    try:
        subprocess.run(z)
        message.ack()
    except Exception:
        traceback.print_exc(file=sys.stderr)
        logging.error(f'missed message {message}')
        message.ack()

    # cleanup directory
    logging.info('copying trace and report files')
    subprocess.run(f'gsutil -h x-goog-meta-bigrna-run:{dat["run"]} cp trace.txt gs://bigrna-cancerdatasci-org/v2/{dat["run"]}/trace.txt', shell=True)
    subprocess.run(f'gsutil -h x-goog-meta-bigrna-run:{dat["run"]} cp report.html gs://bigrna-cancerdatasci-org/v2/{dat["run"]}/report.html', shell=True)
    subprocess.run('rm -rf work', shell=True)
    subprocess.run('rm trace.txt report.html', shell=True)
    
# Substitute PROJECT and SUBSCRIPTION with appropriate values for your
# application.
subscription_path = subscriber.subscription_path(Config.PROJECT_ID, Config.PUBSUB_SUBSCRIPTION)

# Open the subscription, passing the callback.
fc = pubsub.types.FlowControl(max_messages=1, max_lease_duration=7*24*60*60) #7 days
logging.info(f'awaiting messages')
future = subscriber.subscribe(subscription_path, callback, flow_control=fc)

if __name__ == '__main__':
    future.result()
