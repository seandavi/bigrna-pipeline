from google.cloud import pubsub

import subprocess
import logging

import pkg_resources

subscriber = pubsub.SubscriberClient()

PROJECT = 'isb-cgc-01-0006'
SUBSCRIPTION = 'my-sub'

def make_command_line(experiment: str, index: str, gtf: str):
    nf_loc = pkg_resources.resource_filename('bigrna_pipeline', 'main.nf')
    return f'./nextflow run {nf_loc} --experiment {experiment} --with-trace --with-report report.html --index {index} --gtf {gtf} --transcript_version v32'.split()


# Define the callback.
# Note that the callback is defined *before* the subscription is opened.
def callback(message):
    print(message)  # Replace this with your actual logic.
    idx = 'gencode.v32.all_transcripts.k31'
    gtf = 'gencode.v32.annotation.gtf.gz'
    z = make_command_line(message.data.decode('UTF-8'), idx, gtf)
    logging.info(f'got new message {message}')
    try:
        proc = subprocess.Popen(z)
        while True:
            res = proc.poll()
            if(res is None):
                time.sleep(60)
                message.modify_ack_deadline(120)
                continue
            if(res == 0):
                message.ack()  # Asynchronously acknowledge the message.
                break
            else:
                message.nack()
                logging.error('message {message} returned non-zero return code')
    except Exception:
        logging.error(f'missed message {message}')
        message.nack()

    # cleanup directory
    subprocess.run('rm -rf work', shell=True)
    
# Substitute PROJECT and SUBSCRIPTION with appropriate values for your
# application.
subscription_path = subscriber.subscription_path(PROJECT, SUBSCRIPTION)

# Open the subscription, passing the callback.
fc = pubsub.types.FlowControl(max_messages=1)
logging.info(f'awaiting messages')
future = subscriber.subscribe(subscription_path, callback, flow_control=fc)

if __name__ == '__main__':
    future.result()
