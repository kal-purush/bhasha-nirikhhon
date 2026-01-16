import argparse
import gzip
from collections import defaultdict
import pandas as pd 
import json
import sys 
import numpy as np

def get_annotated_counts(splice_junctions, annotated_junctions):
    """If junction is annotated, note the max number of reads that align to each junction stop/start in all samples that
    carry it. E.g. if both of the following junctions were annotated:
        2:100-250 Beryl:20, Besse:10
        2:100-360 Beryl:30, Besse 4
    then the dictionary created should look like this:
        {'2:100' :{'Beryl':30,'Besse':10},  2:250: {'Beryl':20,'Besse':10}, 2:360: {'Beryl':30, 'Besse': 4}"""
    # {"chromosome:position": sample : times_seen_in_sample}
    annotated_counts = defaultdict(lambda: defaultdict(lambda: int))
    for junction in splice_junctions:
        chrom, start, stop, samptimes = junction.get('chrom'), junction.get('start'), junction.get('stop'),\
                                        junction.get('samptimes')
        junction_string = '{}:{}-{}'.format(chrom, start, stop)
        if junction_string in annotated_junctions:  # only consider both annotated

            for pos in [start, stop]:
                pos = "{}:{}".format(chrom, pos)
                for sample, count in samptimes.items():
                    existing_count_for_sample_at_pos = annotated_counts[pos].get(sample)
                    if not existing_count_for_sample_at_pos:
                        annotated_counts[pos][sample] = count
                    elif existing_count_for_sample_at_pos and count > existing_count_for_sample_at_pos:
                        annotated_counts[pos][sample] = count
    return annotated_counts


def normalize_counts(splice_junctions, annotated_counts, sample_ids, annotated_junctions):
    print('Gene	Type\tChrom\tStart\tEnd\tTotalCount\tSampleCount\t{}\tAnnotations\tSeenInTranscriptsList\t{}'.format(
        '\t'.join([s.strip() for s in sample_ids]),
        '\t'.join(['{}_normed'.format(s.strip()) for s in sample_ids])
    ))
    for junction in splice_junctions:
        normalized_dict = defaultdict(int)

        gene, gene_type, chrom, start, stop, ntimes, nsamp, samptimes = junction.get('gene'), \
                                                                        junction.get('gene_type'), \
                                                                        junction.get('chrom'), junction.get('start'), \
                                                                        junction.get('stop'), junction.get('ntimes'),\
                                                                        junction.get('nsamp'), junction.get('samptimes')

        sample_counts_sorted = [samptimes.get(s, 0) for s in sample_ids]
        annotated_start = annotated_counts.get('{}:{}'.format(chrom, start))
        annotated_stop = annotated_counts.get('{}:{}'.format(chrom, stop))
        junction_string = '{}:{}-{}'.format(chrom, start, stop)
        seenBefore = "0"
        if junction_string in annotated_junctions:
            seenBefore = "1"

        tag = ''

        if annotated_start or annotated_stop:
            # Canonical splicing and exon skipping -- both junction start and stop sites are annotated / known
            if annotated_start and annotated_stop:
                tag = "Both annotated"
                for sample in sample_ids:
                    count = samptimes.get(sample, 0)
                    annotated_start_sample_count = annotated_start.get(sample, 0)
                    annotated_stop_sample_count = annotated_stop.get(sample, 0)
                    denominator = max(annotated_start_sample_count, annotated_stop_sample_count)
                    try:
                        normalized = round(float(count)/denominator, 3)
                    except ZeroDivisionError:
                        normalized = "".join([str(count), "*"])
                    normalized_dict[sample] = normalized

            # If only one end of splice junction is annotated, this implies exon extension / intron inclusion
            # where the other end of the junction is not a canonically known splice site
            elif annotated_start or annotated_stop:
                tag = "One annotated"
                for sample in sample_ids:
                    count = samptimes.get(sample, 0)
                    denominator = annotated_start.get(sample, 0) if annotated_start else annotated_stop.get(sample, 0)
                    try:
                        normalized = round(float(count)/denominator, 3)
                    except ZeroDivisionError:
                        normalized = "".join([str(count), "*"])
                    normalized_dict[sample] = normalized

            normalized_cols_sorted = [normalized_dict[s] for s in sample_ids]
            line_to_print = "\t".join(
                stringify_list_contents([gene, gene_type, chrom, start, stop, ntimes, nsamp,
                 "\t".join(stringify_list_contents(sample_counts_sorted)),
                 tag,
                 seenBefore,
                 "\t".join(stringify_list_contents(normalized_cols_sorted))])
            )
            print(line_to_print)

        # If neither junction is annotated then simply just print out the original splice junction line
        elif annotated_stop is None and annotated_stop is None:
            tag = "Neither annotated"
            line_to_print = "\t".join(
                stringify_list_contents([gene, gene_type, chrom, start, stop, ntimes, nsamp,
                 "\t".join(stringify_list_contents(sample_counts_sorted)),
                 tag,
                 seenBefore,
                 '\t'.join(["" for _ in sample_ids])])
            )
            print(line_to_print)


def stringify_list_contents(x):
    return [str(s) for s in x]


def get_junctions(splice_file, sample_ids):
    """
    :param splice_file: The output file from the SpliceJunctionDiscovery.py script
    :return: a list of dicts representing junctions extracted from the file.
    """
    junctions = []

    with open(splice_file, 'r') as f:
        splice_file_lines = f.readlines()[1:]  # Start at 1 index to skip header line
        for splice_file_line in splice_file_lines:
            gene, gene_type, chrom, start, stop, ntimes, nsamp = splice_file_line.strip().split("\t")[0:7]
            sample_counts = [int(c.strip()) for c in splice_file_line.split("\t")[7:]]
            junctions.append({
                'gene': gene,
                'gene_type': gene_type,
                'chrom': chrom,
                'start': start,
                'stop': stop,
                'ntimes': ntimes,
                'nsamp': nsamp,
                'samptimes': {k: v for k, v in zip(sample_ids, sample_counts)}
            })
    return junctions


def make_annotated_junction_set(transcript_model, chrom_col, start_col, stop_col):
    """From an annotations file, extract all annotated splice junctions, including every permutation with 1 bp flank on
    either side. For example, if a junction is annotated as 1:1221-1345, then we will include not only that version of
    it but also 1:1222-1346, 1:1220-1344, 1:1220-1346 and 1:1222-1344 to account for potential off-by-one indexing
    differences."""
    annotated_junctions = set()

    for junction in transcript_model:
        fields = junction.strip().split("\t")

        chrom = fields[chrom_col].strip("chr")
        start = int(fields[start_col])
        stop = int(fields[stop_col])

        # shifts the junction while maintaining the same distance between start and stop
        for offset in range(-1, 2):  # [-1,0,1]
            start_flank = start + offset

            # Offset by one because gencode end positions are 2 positions into the exon
            # -- we want the first base of the exon (non-inclusive end position for intron)
            stop_flank = stop -1 + offset
            junction = "{}:{}-{}".format(chrom, start_flank, stop_flank)
            annotated_junctions.add(junction)

        # generate junctions with the most extreme flanking regions of start and stop
        outer_junction = "{}:{}-{}".format(chrom, start - 1, stop + 1)
        inner_junction = "{}:{}-{}".format(chrom, start + 1, stop - 1)
        annotated_junctions.add(outer_junction)
        annotated_junctions.add(inner_junction)

    return annotated_junctions


def main(args):

    sys.stdout = open("{}/{}_outputs.txt".format(args.output_folder, args.sample_set_id), "a")

    if args.gzipped:
        transcript_model = gzip.open(args.transcript_model)
    else:
        transcript_model = open(args.transcript_model)

    annotated_junction_set = make_annotated_junction_set(transcript_model,
                                                         args.chrom_col,
                                                         args.start_col,
                                                         args.stop_col)

    if args.normalize:
        
        #with open(args.splice_file, 'r') as f:   ###
            #sample_ids = f.readline().split("\t")[7:]
        
        splice_df = pd.read_csv(args.splice_file, sep='\t')

        if "samptimes" in list(splice_df.columns.values):
            samptimes_list = [json.loads(k) for k in splice_df.samptimes.values]
            sample_ids = np.unique([k for d in samptimes_list for k in d.keys()])
            sample_ids = [str(s_i) for s_i in sample_ids]
            if len(sample_ids) > 1:
                splice_df['samptimes'] = splice_df['samptimes'].apply(lambda x: json.loads(x))
                splice_junctions = splice_df.to_dict('records')

                annotated_counts = get_annotated_counts(splice_junctions, annotated_junction_set)
                normalize_counts(splice_junctions, annotated_counts, sample_ids, annotated_junction_set)

        #else:
            #sample_ids = f.readline().split("\t")[7:]
            #splice_junctions = get_junctions(args.splice_file, sample_ids)
        
        #annotated_counts = get_annotated_counts(splice_junctions, annotated_junction_set)
        #normalize_counts(splice_junctions, annotated_counts, sample_ids, annotated_junction_set)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='''Get unannotated junctions from splice file''')
    parser.add_argument('-transcript_model',
                        help="Transcript model of canonical splicing, e.g. gencode v19. Default is set to /Users/alice/Desktop/VALabResearch/code/SpliceJunctionDiscovery/reference/gencode.comprehensive.splice.junctions.txt",
                        action='store',
                        default="/Users/alice/Desktop/VALabResearch/code/SpliceJunctionDiscovery/reference/gencode.comprehensive.splice.junctions.txt")
    parser.add_argument('-splice_file', help='Splice junction file to filter')
    parser.add_argument('-gzipped', help='Add if sjout file is gzipped', action='store_true')
    parser.add_argument('-chrom_col', help='Chromosome column', type=int, default=0)
    parser.add_argument('-start_col', help='Junction start column', type=int, default=1)
    parser.add_argument('-stop_col', help='Junction stop column', type=int, default=2)
    parser.add_argument('-sample_set_id', help='Sample Set ID', type=str, default='x')
    parser.add_argument('-output_folder', help='output folder', type=str, default='outputs')
    mode_arguments = parser.add_mutually_exclusive_group(required=True)
    mode_arguments.add_argument('--normalize', action='store_true',
                                help='Local normalization on splice junction file based on annotated junctions')
    args = parser.parse_args()
    main(args)