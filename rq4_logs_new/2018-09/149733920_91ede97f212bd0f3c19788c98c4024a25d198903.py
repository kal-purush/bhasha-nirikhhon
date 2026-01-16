#result RAW file collecting code (ver.STAR-Fusion)
$ ls > SKCM_sample.txt

import os,re
cancer = 'SKCM'
maindir = '/storage2/Project/TCGA_fusion/STARFusion/%s'%cancer
sample = open("%s/%s_sample.txt"%(maindir,cancer),'r')
sample_list = set([line.rstrip("\n") for line in sample.readlines()])

os.system("rm %s/result/%s_result.txt"%(maindir,cancer))

for i in sample_list:
#	os.system("cp %s/%s/star-fusion.fusion_candidates.final %s/%s/star-fusion.fusion_predictions.abridged.tsv"%(maindir,i,maindir,i))
	os.system("cat %s/%s/star-fusion.fusion_predictions.abridged.tsv > %s/result/%s.txt"%(maindir,i,maindir,i))
	os.system("ls %s/result/%s.txt >> %s/result/%s_result.txt"%(maindir,i,maindir,cancer))
	os.system("cat %s/result/%s.txt | cut -f1-3 >> %s/result/%s_result.txt"%(maindir,i,maindir,cancer))

os.system("sed '/#/d' %s/result/%s_result.txt > %s/result/1.txt"%(maindir,cancer,maindir))
os.system("sed 's/\/storage2\/Project\/TCGA_fusion\/STARFusion\/%s\/result\///g' %s/result/1.txt > %s/result/%s_result.txt"%(cancer,maindir,maindir,cancer))

result = open("%s/result/%s_result.txt"%(maindir,cancer),'r') 
result_lines = result.readlines()
sample = re.compile("^TCGA")
result_table = open("%s/result/%s_result_table.txt"%(maindir,cancer),'w')
#sample_ID   fusion   JunctionReadCount   SpanningFragCount
for i in result_lines:
	sample_check = sample.match(i)
	if sample_check :
		sample_ID = i.rstrip(".txt\n")
	else :
		result_table.write("%s\t%s"%(sample_ID,i))

result.close()
result_table.close()

result_T = open("%s/result/%s_result_table.txt"%(maindir,cancer),'r') 
result_Tlines = result_T.readlines()
filtered_result = open("%s/result/%s_filtered_result.txt"%(maindir,cancer),'w')
for i in result_Tlines:
	sample = i.split('\t')[0]
	fusion = i.split('\t')[1]
	seed = int(i.split('\t')[2])
	spanning = int(i.split('\t')[3].rstrip('\n'))
	if (seed == 1 & spanning >= 2) | seed >= 2:
		filtered_result.write("%s"%i)

result_T.close()
filtered_result.close()
os.system("cut -f2 %s/result/%s_filtered_result.txt | sort | uniq -dc | sort > %s/result/%s_duplicate.txt"%(maindir,cancer,maindir,cancer))



filtered_result = open("%s/result/%s_filtered_result.txt"%(maindir,cancer),'r')
Flines = filtered_result.readlines()
for i in Flines:
	sample = i.split('\t')[0]
	fusion = i.split('\t')[1]
	H_gene = fusion.split('--')[0]
	T_gene = fusion.split('--')[1]
	if H_gene 

#######
#(ver.FusionScan)
import os 

cancer = 'SKCM'
maindir = '/storage/home/subin95/FusionScan_TCGA/%s/result'%cancer
sample = open("%s/%s_sample.txt"%(maindir,cancer),'r')
sample_list = set([line.rstrip("\n") for line in sample.readlines()])

for i in sample_list:
	os.system("cat %s/%s/%s_FusionScan_candidate_result.txt > %s/result/%s.txt"%(maindir,i,i,maindir,i))




import os 

cancer = 'KIRC'
main_dir = '/storage/home/subin/Fusionscan_TCGA/%s/result'%cancer

os.system("cd %s"%main_dir)
os.system("ls -d TCGA* > %s/%s.txt"%(main_dir,cancer))

sample = open("%s/%s.txt"%(main_dir,cancer))
sample_list = set([line.rstrip("\n") for line in sample.readlines()])

for i in sample_list:
	os.system("rm -r %s/%s/*_1"%(main_dir,i))
	os.system("rm -r %s/%s/*_2"%(main_dir,i))
	os.system("rm -r %s/%s/*_3"%(main_dir,i))
	os.system("rm -r %s/%s/*_4"%(main_dir,i))
	os.system("rm -r %s/%s/*_5"%(main_dir,i))
	os.system("rm -r %s/%s/*_6"%(main_dir,i))
	os.system("rm -r %s/%s/*_7"%(main_dir,i))
	os.system("rm -r %s/%s/*_8"%(main_dir,i))
	os.system("rm -r %s/%s/fa_dir"%(main_dir,i))
	os.system("rm -r %s/%s/genefa_dir"%(main_dir,i))
	os.system("rm -r %s/%s/log_dir"%(main_dir,i))
	os.system("rm -r %s/%s/pslx_dir"%(main_dir,i))
	os.system("rm -r %s/%s/rescue_dir"%(main_dir,i))
	os.system("rm -r %s/%s/temp_dir"%(main_dir,i))


################################################################3
#KOBIC result re_naming
#######################
$ ls > PAAD_file.txt

import os,glob

main_dir = '/storage2/Project/TCGA_fusion/STARFusion'
cancer = 'PAAD' #암종명
jason = 'files.2018-08-16.json' 
tsv = '%s.tsv'%cancer
manifest = open('%s/%s/gdc_manifest_20180816_120356.txt'%(main_dir,cancer),'r')
manifest.readline()
manifest_lines = manifest.readlines()

Mdict = {}
for i in manifest_lines:
	folder = i.split('\t')[0]
	file = i.split('\t')[1]
	Mdict[folder] = file

manifest.close()

Gdict = {}
gdc_legacy = open("%s/%s/%s"%(main_dir,cancer,jason))
for line in gdc_legacy:
    try:
        if 'file_name' in line:
            tar = line.split('"file_name": "')[1].split('",')[0]
        elif 'case_id' in line:
            uuid = line.split('"case_id": "')[1].split('"')[0]
        Gdict[tar]=uuid
    except:
        print line

gdc_legacy.close()

Bdict={}
gdc = open("%s/%s/%s"%(main_dir,cancer,tsv))
gdc_lines = gdc.readlines()
for line in gdc_lines:
    try: 
        uuid = line.split('\t')[1]
        bar = line.split('\t')[2]
        Bdict[uuid]=bar
    except:
        print line

gdc.close()

result = open('%s/%s/%s_file.txt'%(main_dir,cancer,cancer),'r')
result_list = set([line.rstrip("\n") for line in result.readlines()])
for i in result_list:
	os.system("mv %s/%s/%s %s/%s/%s"%(main_dir, cancer, i ,main_dir, cancer, Bdict[Gdict[Mdict[i]]]))












