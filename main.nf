#!/usr/bin/env nextflow
import groovy.json.JsonSlurper

def jsonSlurper = new JsonSlurper()

res = jsonSlurper.parse(new URL('https://api.omicidx.cancerdatasci.org/sra/experiments/' + params.experiment + '/runs?size=500'))

println(res)

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
 taxon_id             := ${params.transcript_version}
 ====================================

 """


process produceSequences {
    tag { srr }
    cpus 8
    memory "64GB"
    time '6h'
    queue "ccr,norm"
    module "sratoolkit/2.9.2"
    module "aws"
    clusterOptions " --gres lscratch:200"
    // publishDir "s3://starbuck1/tmp/${srr}", mode: 'copy'

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
      n=$[$n+1]
      sleep 15
    done
    '''

}

se = Channel.create()

records.groupTuple().into(se)

process salmon {
    tag { srx }
    container 'combinelab/salmon'
    cpus 24
    time '8h'
    memory '16GB'
    module "salmon/0.11.3"

    publishDir "s3://s3.bigrna.cancerdatasci.org/results/${species}/${transcript_version}"

    input:
    set srx, file(abc) from se
    file(idx) from idx
    file(gtf) from gtf

    output:
        file("${srx}/*") into quants

    shell:
    r = wrap_items(abc)
    """
    salmon quant -p ${task.cpus} -g ${gtf} --gcBias --seqBias --numBootstraps 25 --index ${idx} -l A -o ${srx}`python /data/sdavis2/projects/BigRNA_new/big_rna/make_salmon_read_string.py ${r}`
    gzip ${srx}/quant.sf
    gzip ${srx}/quant.genes.sf
    gzip ${srx}/aux_info/ambig_info.tsv
    """
}

quants.subscribe{ println("$it") }
