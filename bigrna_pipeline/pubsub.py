from google.cloud import pubsub_v1

import subprocess
import shlex
import logging
import traceback
import sys
import json
import os
import time

import pkg_resources

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

subscriber = pubsub_v1.SubscriberClient()


class Config():
    DATABASE_URL = os.environ.get('DATABASE_URL')
    PROJECT_ID = os.environ.get("PROJECT_ID")
    PUBSUB_SUBSCRIPTION = os.environ.get("PUBSUB_SUBSCRIPTION")
    PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC")

    
def get_subscription_path():
    return subscriber.subscription_path(Config.PROJECT_ID, Config.PUBSUB_SUBSCRIPTION)


def make_command_line(experiment: str, run_id: str, index: str, gtf: str):
    nf_loc = pkg_resources.resource_filename('bigrna_pipeline', 'main.nf')
    return shlex.split(f'./nextflow run {nf_loc} --run_id {run_id} --experiment {experiment} -with-trace -with-report report.html --index {index} --gtf {gtf} --transcript_version v32')
    #return(['sleep','20'])


def start_nextflow_subprocess(message) -> subprocess.Popen:
    logging.info('handling message %s', message)
    dat = json.loads(message.data.decode('UTF-8'))
    idx = 'gencode.v32.all_transcripts.k31'
    gtf = 'gencode.v32.annotation.gtf.gz'
    z = make_command_line(dat['accession'], dat['run'], idx, gtf)
    return subprocess.Popen(z, stderr=subprocess.PIPE, stdout=subprocess.PIPE)


def get_next_message() -> pubsub_v1.types.message:
    subscription_path = get_subscription_path()
    response = subscriber.pull(
        subscription_path,
        max_messages = 1,
        timeout = 30.0 # see https://github.com/googleapis/google-cloud-python/issues/9390#issuecomment-561883278
    )
    # should be only one message
    message = None
    for r in response.received_messages:
        message = r
    return message


def write_std_files(process: subprocess.Popen):
    def get_stream_as_str(stream) -> str:
        return stream.read().decode('UTF-8')
    stdout_str = get_stream_as_str(process.stdout)
    stderr_str = get_stream_as_str(process.stderr)
    with open('stdout.txt', 'w') as f:
        f.write(stdout_str)
    sys.stdout.writelines(stdout_str)
    with open('stderr.txt', 'w') as f:
        f.write(stderr_str)
    sys.stderr.write(stderr_str)
    

def failed_pipeline(process: subprocess.Popen, message: pubsub_v1.types.message):
    logging.error(f'{message} failed with exit code {process.poll()}')
    with open('failed.txt', 'w') as f:
        f.write(f'{message} failed with exit code {process.poll()}')
    

def succeeded_pipeline(process: subprocess.Popen, message: pubsub_v1.types.message):
    logging.info(f'{message} succeeded with exit code {process.poll()}')
    with open('success.txt', 'w') as f:
        f.write(f'{message} succeeded with exit code {process.poll()}')
    

def cleanup(message):
    # cleanup directory
    dat = json.loads(message.data.decode('UTF-8'))
    logging.info('Finishing up....')
    files_to_capture = [
        'success.txt',
        'failed.txt',
        'stdout.txt',
        'stderr.txt',
        'trace.txt',
        'report.html'
    ]
    for fname in files_to_capture:
        subprocess.run(f'gsutil -h x-goog-meta-bigrna-run:{dat["run"]} cp {fname} gs://bigrna-cancerdatasci-org/v2/{dat["run"]}/{fname}', shell=True)
    subprocess.run('rm -rf work', shell=True)
    subprocess.run('rm '+' '.join(files_to_capture), shell=True)
        
    
def run_to_death():
    subscription_path = get_subscription_path()
    while True:
        message = get_next_message()
        if(message is None):
            logging.info('Message queue appears to be empty')
            break
        process = start_nextflow_subprocess(message.message)
        n = 0
        SLEEP_INTERVAL=10
        while process.poll() is None:
            subscriber.modify_ack_deadline(
                subscription_path,
                [message.ack_id],
                ack_deadline_seconds=480,
            )
            time.sleep(SLEEP_INTERVAL)
            if(n % 120 == 0):
                logging.info('waiting for process to complete')
            n+=SLEEP_INTERVAL
            # sys.stderr.write('.')
            # sys.stderr.flush()
        if process.poll() == 0:
            succeeded_pipeline(process, message.message)
        else:
            failed_pipeline(process, message.message)
        subscriber.acknowledge(subscription_path, [message.ack_id])
        logging.info(f'acked {message.message}')
        write_std_files(process)
        cleanup(message.message)

# Substitute PROJECT and SUBSCRIPTION with appropriate values for your
# application.

if __name__ == '__main__':
    run_to_death()
