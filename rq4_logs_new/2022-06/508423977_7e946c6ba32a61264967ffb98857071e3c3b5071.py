import sys
import argparse
import datetime

parser = argparse.ArgumentParser(description = "SV2 genotyper")
# From make_feature_table.py step
parser.add_argument("--alignment_file", help = "CRAM/BAM file input")
parser.add_argument("--reference_fasta", help = "Reference fasta file input")
parser.add_argument("--snv_vcf_file", help = "SNV VCF file input")
parser.add_argument("--regions_bed", help = "BED file with pre-generated random genomic regions (for estimating coverage per chromosome)")
parser.add_argument("--exclude_regions_bed", help = "BED file with pre-generated random genomic regions (for estimating coverage per chromosome)")
parser.add_argument("--sv_bed_file", help = "SV BED file input")
parser.add_argument("--sv_feature_output_tsv", help = "Feature table .tsv output (it will be given the default name if this arugment isn't set)")
parser.add_argument("--preprocessing_table_input", help = "A pre-generated preprocessing table (if SV2 had been run before and you want to skip the preprocessing part of the program")
parser.add_argument("--preprocessing_table_output", help = "Preprocessing table .tsv output (it will be given the default name if this argument isn't set)")
parser.add_argument("--gc_reference_table", help = "GC content reference table input")
# From classify.py step
parser.add_argument("--sex", help = "Sex of sample: male or female")
parser.add_argument("--clf_folder", help = "Folder that contains the classifiers (classifiers must be in .pkl format)")
parser.add_argument("--genotype_predictions_output_tsv", help = "Output features .tsv file with genotype predictions")
# From make_vcf.py step
parser.add_argument("--sample_name", help = "Sample name")
parser.add_argument("--output_vcf", help = "Output VCF filepath (optional)")
args = parser.parse_args()

# Chromosomes to analyze (1,... 22, X, Y)
chroms = list(map(str, np.arange(1, 22 + 1)))
chroms.extend(["X", "Y"])

# SV types to classify
svtypes = ("DEL", "DUP")

# Make the GC content reference table
GC_content_reference_table = make_GC_content_reference_table(GC_content_reference_table_filepath)
# Make the regions table (used to estimate coverage)
regions_table = make_regions_table(args.regions_bed)

if not args.preprocessing_table_input:
    # Make the CRAM/BAM preprocessing table
    alignment_preprocessing_table = make_alignment_preprocessing_table(args.alignment_file, args.reference_fasta)
    
    # Make the SNV preprocessing table
    snv_preprocessing_table = get_snv_preprocessing_data(args.snv_vcf_file)

    # Merge and output the preprocessing table
    df_alignment_preprocessing_table = pd.DataFrame.from_dict(alignment_preprocessing_table, orient = "index")
    df_snv_preprocessing_table = pd.DataFrame.from_dict(snv_preprocessing_table, orient = "index")
    df_preprocessing_table = df_alignment_preprocessing_table.join(df_snv_preprocessing_table).reset_index(level = 0).rename(columns = {"index": "chrom"})
    df_preprocessing_table.to_csv(args.preprocessing_table_output, sep = "\t", index = False)
else:
    df_preprocessing_table = pd.read_csv(args.preprocessing_table_input, sep = "\t")

# Preprocess the input SV BED file
sv_bed_list = []
with open(args.sv_bed_file, "r") as f:
    for line in f:
        linesplit = line.rstrip().split("\t")
        chrom, start, stop, features = linesplit[0], int(linesplit[1]), int(linesplit[2]), linesplit[3]
        if start > stop: continue
        sv_bed_list.append(line.rstrip())
sv_bed = BedTool(sv_bed_list).filter(lambda x: len(x) > 0).saveas()
exclude_bed = BedTool(args.exclude_regions_bed).merge()
sv_interval_table = make_sv_interval_table(sv_bed, exclude_bed)

# Make SNV features table (for each filtered SV call)
snv_features_table = make_snv_features_table(args.snv_vcf_file, sv_bed)

# Make CRAM/BAM features table (for each filtered SV call)
alignment_features_table = make_alignment_features_table(args.alignment_file, args.reference_fasta, sv_bed)

# Merge and output feature table
df_alignment_features_table = pd.DataFrame.from_dict(alignment_features_table, orient = "index")
df_snv_features_table = pd.DataFrame.from_dict(snv_features_table, orient = "index")
df_features_table = df_alignment_features_table.join(df_snv_features_table).reset_index().rename(columns = {"level_0": "chrom", "level_1": "start", "level_2": "end"})

if not args.sv_feature_output_tsv: 
    from time import gmtime, strftime
    current_time = strftime("%Y-%m-%d_%H.%M.%S", gmtime())
    df_features_table.to_csv("sv2_features.{}.tsv".format(current_time), sep = "\t", index = False)
else: df_features_table.to_csv(args.sv_feature_output_tsv, sep = "\t", index = False)

df, df_male_sex_chromosomes = load_features_from_dataframe(df_features_table, args.sex)

# Set default filepaths for the classifiers
clf_highcov_del_gt1kb_filepath = "data/trained_classifiers/clf_del_gt1kb.pkl"
clf_highcov_del_lt1kb_filepath = "data/trained_classifiers/clf_del_lt1kb.pkl"
clf_dup_breakpoint_filepath = "data/trained_classifiers/clf_dup_breakpoint.pkl"
clf_dup_har_filepath = "data/trained_classifiers/clf_dup_har.pkl"
clf_del_malesexchrom_filepath = "data/trained_classifiers/clf_del_malesexchrom.pkl"
clf_dup_malesexchrom_filepath = "data/trained_classifiers/clf_dup_malesexchrom.pkl"

# If classifier folder is provided as argument, use the ones inside of that folder (must have the same names as default classifiers)
if args.clf_folder:
    clf_highcov_del_gt1kb_filepath = "{}/clf_del_gt1kb.pkl".format(args.clf_folder)
    clf_highcov_del_lt1kb_filepath = "{}/clf_del_lt1kb.pkl".format(args.clf_folder)
    clf_dup_breakpoint_filepath = "{}/clf_dup_breakpoint.pkl".format(args.clf_folder)
    clf_dup_har_filepath = "{}/clf_dup_har.pkl".format(args.clf_folder)
    clf_del_malesexchrom_filepath = "{}/clf_del_malesexchrom.pkl".format(args.clf_folder)
    clf_dup_malesexchrom_filepath = "{}/clf_dup_malesexchrom.pkl".format(args.clf_folder)


# Autosomal SV classifiers
df_highcov_del_gt1kb_preds = run_highcov_del_gt1kb_classifier(df, clf_highcov_del_gt1kb_filepath)
df_highcov_del_lt1kb_preds = run_highcov_del_lt1kb_classifier(df, clf_highcov_del_lt1kb_filepath)
df_dup_breakpoint_preds = run_dup_breakpoint_classifier(df, clf_dup_breakpoint_filepath)
df_dup_har_preds = run_dup_har_classifier(df, clf_dup_har_filepath)

# Concat and sort output diploid dfs
df_preds_concat_sorted = concat_and_sort_pred_dfs([df_highcov_del_gt1kb_preds, df_highcov_del_lt1kb_preds, df_dup_breakpoint_preds, df_dup_har_preds], df)

# Male sex chromosome SV classifiers
df_malesexchrom_del_preds = run_malesexchrom_del_classifier(df_male_sex_chromosomes, clf_del_malesexchrom_filepath) 
df_malesexchrom_dup_preds = run_malesexchrom_dup_classifier(df_male_sex_chromosomes, clf_dup_malesexchrom_filepath) 

# Concat and sort output haploid dfs
df_malesexchrom_preds_concat_sorted = concat_and_sort_pred_dfs([df_malesexchrom_del_preds, df_malesexchrom_dup_preds], df_male_sex_chromosomes)

# Concat the diploid and haploid dfs, and then add quality scores
df_preds_concat_sorted = pd.concat([df_preds_concat_sorted, df_malesexchrom_preds_concat_sorted])
df_preds_concat_sorted["ALT_GENOTYPE_LIKELIHOOD"] = df_preds_concat_sorted["HET_GENOTYPE_LIKELIHOOD"] + df_preds_concat_sorted["HOM_GENOTYPE_LIKELIHOOD"]
df_preds_concat_sorted["REF_QUAL"] = -10.0*np.log10(1.0 - df_preds_concat_sorted["REF_GENOTYPE_LIKELIHOOD"])
df_preds_concat_sorted["ALT_QUAL"] = -10.0*np.log10(df_preds_concat_sorted["REF_GENOTYPE_LIKELIHOOD"])
df_preds_concat_sorted = df_preds_concat_sorted[["chrom", "start", "end", "type", "size", "coverage", "coverage_GCcorrected", "discordant_ratio", "split_ratio", "snv_coverage", "heterozygous_allele_ratio", "snvs", "het_snvs", "ALT_GENOTYPE_LIKELIHOOD", "REF_QUAL", "ALT_QUAL", "HOM_GENOTYPE_LIKELIHOOD", "HET_GENOTYPE_LIKELIHOOD", "REF_GENOTYPE_LIKELIHOOD"]]

# Initialize the GEN column to the missing genotype value ./.
df_preds_concat_sorted["GEN"] = "./."
df_preds_concat_sorted["GEN"] = df_preds_concat_sorted[["REF_GENOTYPE_LIKELIHOOD", "HET_GENOTYPE_LIKELIHOOD", "HOM_GENOTYPE_LIKELIHOOD"]].apply(lambda x: get_genotype(*x), axis = 1)

genotype_table_filepath = str()
if not args.genotype_predictions_output_tsv:
    from time import gmtime, strftime
    current_time = strftime("%Y-%m-%d_%H.%M.%S", gmtime())
    genotype_table_filepath = "genotyping_preds_{}.tsv".format(current_time)
else: 
    genotype_table_filepath = args.genotype_predictions_output_tsv
df_preds_concat_sorted.to_csv(genotype_table_filepath, sep = "\t", index = False)

make_vcf(args.sample_name, args.reference_fasta, genotype_table_filepath, args.output_vcf)