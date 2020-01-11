#!/bin/bash

wget ftp://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_32/gencode.v32.transcripts.fa.gz
wget ftp://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_32/gencode.v32.annotation.gtf.gz

salmon index -t gencode.v32.transcripts.fa.gz --gencode -i gencode.v32.all_transcripts.k31 -k 31 
salmon index -t gencode.v32.transcripts.fa.gz --gencode -i gencode.v32.all_transcripts.k23 -k 23 

