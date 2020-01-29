#!/usr/bin/env nextflow
/*
Usage:

nextflow run {nf_loc} \
  --run_id {run_id} \
  --experiment {experiment} \
  --with-trace \
  --with-report report.html \
  --index {index} \
  --gtf {gtf} \
  --transcript_version v32

 */

import groovy.json.JsonSlurper

def jsonSlurper = new JsonSlurper()

res = jsonSlurper.parse(new URL('https://api.omicidx.cancerdatasci.org/sra/experiments/' + params.experiment + '/runs?size=500'))

def wrap_items(input_files) {
  def result =  input_files instanceof Path ? input_files.toString() : (input_files as List).join(',')
  return result
}

l = res.hits.collect {
    [ it.experiment.accession, it.accession, it.sample.taxon_id ]
}

idx = file(params.index)
gtf = file(params.gtf)

srrs = Channel.from( l )

log.info """\


 B I G  R N A - N F   P I P E L I N E
 ====================================
 transcriptome index  := ${params.index}
 SRA Experiment       := ${params.experiment}
 gtf file             := ${params.gtf}
 transcript version   := ${params.transcript_version}
 run_id               := ${params.run_id}
 ====================================


 """


process produceSequences {
    tag { srr }
    module "sratoolkit"
    cpus 16
    memory "32GB"
    // clusterOptions " --gres lscratch:200"

    input:
    set srx, srr, taxon_id from srrs

    output:
        set val(srx), file("*fastq*") into (records, records2, records3, records4) mode flatten

    shell:
    '''
    #!/bin/bash
    #/home/ubuntu/sratoolkit.2.9.2-ubuntu64/bin/fastq-dump -N 1000 --split-files --gzip $srr
    #mkdir -p /lscratch/${SLURM_JOB_ID}/fasterq
    n=0
    until [ $n -ge 5 ]
    do
      n=0
      fasterq-dump -e !{task.cpus} -f --skip-technical --split-files !{srr}  && break  
      #fastq-dump -X 1000000 --skip-technical --split-files !{srr}  && break  
      n=$[$n+1]
      sleep 15
    done
    '''

}

se = Channel.create()

records.groupTuple().into(se)

process salmon {
    tag { params.run_id }
    cpus 16
    memory '32GB'
    module "salmon/1.0.0"

    //publishDir "gs://temp-testing/results2/${params.run_id}/"

    input:
    set srx, file(abc) from se
    file(idx) from idx
    file(gtf) from gtf

    output:
        file("${params.run_id}/**") into quants

    shell:
    r = wrap_items(abc)
    """
    salmon quant -p ${task.cpus} -g ${gtf} --gcBias --seqBias --biasSpeedSamp 10 --numBootstraps 25 --index ${idx} -l A -o ${params.run_id}`python $workflow.launchDir/make_salmon_read_string.py ${r}`
    gzip ${params.run_id}/quant.sf
    gzip ${params.run_id}/quant.genes.sf
    gzip ${params.run_id}/aux_info/ambig_info.tsv
    find ${params.run_id}/ -type f > ${params.run_id}/manifest.txt
    """
}

process put_files {
    tag { params.run_id }

    input:
    file to_send from quants.flatten()

    shell:
    """
    gsutil -h x-goog-meta-bigrna-run:${params.run_id}  cp ${to_send} gs://bigrna-cancerdatasci-org/v2/${params.run_id}/${to_send}
    """
}

