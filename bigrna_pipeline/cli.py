import subprocess

import click

from .pubsub import run_to_death

@click.group()
def bigrna():
    pass

@bigrna.command(help='download nextflow executable to current directory')
def get_nextflow():
    subprocess.run("wget -qO- https://get.nextflow.io | bash", shell=True)


@bigrna.command(help='start a bigrna pubsub-based worker')
def start_worker():
    run_to_death()

@bigrna.command()
def check_loc():
    import pkg_resources
    print(pkg_resources.resource_filename('bigrna_pipeline', 'main.nf'))
    
if __name__ == '__main__':
    bigrna()
