from google.cloud import pubsub

import subprocess
import logging
import traceback
import sys
import json

import pkg_resources

subscriber = pubsub.SubscriberClient()

PROJECT = 'isb-cgc-01-0006'
SUBSCRIPTION = 'my-sub'

def make_command_line(experiment: str, run_id: str, index: str, gtf: str):
    nf_loc = pkg_resources.resource_filename('bigrna_pipeline', 'main.nf')
    return f'./nextflow run {nf_loc} --run_id {run_id} --experiment {experiment} --with-trace --with-report report.html --index {index} --gtf {gtf} --transcript_version v32'.split()

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
    subprocess.run('rm -rf work', shell=True)
    
# Substitute PROJECT and SUBSCRIPTION with appropriate values for your
# application.
subscription_path = subscriber.subscription_path(PROJECT, SUBSCRIPTION)

# Open the subscription, passing the callback.
fc = pubsub.types.FlowControl(max_messages=1, max_lease_duration=18000)
logging.info(f'awaiting messages')
future = subscriber.subscribe(subscription_path, callback, flow_control=fc)

if __name__ == '__main__':
    future.result()
