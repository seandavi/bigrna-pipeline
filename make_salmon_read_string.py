#!/usr/bin/env python
import sys
import re
import collections

def make_fastq_samplemap(fastqs, sample_regex = r'([^._]+).*'):
    """make a map of fastq names to samples (base names)
    
    Parameters
    ----------
    fastqs : list(str)
        A list of fastq files.
    sample_regex : regex
        This regex is used to capture the "sample" from the fastq 
        files. It should return as its first group the fastq grouping
        for paired- or single-end libraries.

        For example, for SRA, the regex is '([^._]+).*' since 
        fastq files look like SRR123456_1.fastq(.gz) or 
        SRR123457.fastq(.gz). The results of re.match will
        then be ('SRR123456',) and ('SRR123457',), respectively.

    Returns
    -------
    A map of <samplename, [fastqs]>

    >>> 
    """
    samplemap = collections.defaultdict(list)
    for fastq in fastqs:
        samplename = re.match(sample_regex, fastq).groups(0)[0]
        samplemap[samplename].append(fastq)
    return samplemap

def make_salmon_read_map(samplemap):
    reads = {'-1': [], '-2': [], '-r': []}
    for sample, fastqs in samplemap.items():
        if(len(fastqs)==2):
            fastqs = sorted(fastqs)
            reads['-1'].append(fastqs[0])
            reads['-2'].append(fastqs[1])
        else:
            reads['-r'].append(fastqs[0])
    return reads

def make_salmon_read_call_string(readmap):
    readstring = ""
    for readnum, fastqs in readmap.items():
        if(len(fastqs)>0):
            readstring = readstring + " " + readnum + " " + " ".join(fastqs)
    return readstring

def main():
    if(len(sys.argv)!=2):
        print("""Error:
Usage: python prep_salmon_reads.py COMMA_SEPARATED_FASTQ_FILES
       python prep_salmon_reads.py SRR123.fastq,SRR1234_1.fastq,SRR1234_2.fastq,SRR12345_2.fastq,SRR12345_3.fastq

output:  -1 SRR1234_1.fastq SRR12345_2.fastq -2 SRR1234_2.fastq SRR12345_3.fastq -r SRR123.fastq
""")
        exit(1)
    fastqs = sys.argv[1].split(',')
    samplemap = make_fastq_samplemap(fastqs)
    readmap   = make_salmon_read_map(samplemap)
    print(make_salmon_read_call_string(readmap))

if __name__ == '__main__':
    main()




