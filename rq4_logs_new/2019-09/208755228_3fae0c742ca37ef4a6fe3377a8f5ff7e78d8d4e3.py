# -*- coding: utf-8 -*-
'''
Usage:
python filter_genewise_gff_redundancy.py inputfile filter_num outfile
python /share/home/baishenglong/programs/my_script/gene_ann_majianchao/filter_genewise_gff_redundancy.py result/Oryza_sativa.pep.all.fa.genblast.genewise.filter.gff 50 filter_genewise_gff.py ./Oryza_sativa.pep.all.fa.genblast.genewise.gff.new

对genewise产生的gff文件进行去冗余。
冗余标准为若两个mRNA有100bp以上的overlap，则保留分值较高的，删除分值较低的。

脚本有以下几个步骤：
1. 读入gff文件，将信息记录入 dir_allgene，dir_chr_mRNA 两个字典内。
dir_allgene以geneid为键，gene对应的mRNA和CDS信息为值
dir_chr_mRNA 以contig/Scaffold信息为键，mRNA信息为值

2. 整理dir_chr_mRNA 格式，整理后每个contig/scaffold信息写入 dir_chrx_mRNA_arrange 新字典内，以进行下一步过滤
键为geneid，值为对应的mRNA信息

3. 调用filter_write函数对每一个dir_chrx_mRNA_arrange内mRNA进行过滤

'''
import sys,time,os, progressbar

inputfile = sys.argv[1]
filter_num = float(sys.argv[2])
outfile = sys.argv[3]

def filter_write(dir_mRNA):
    # 初始化dir_needmRNA，目的是进入双重循环的第二重循环
    # 该字典后续用于记录需要的mRNA条目信息。geneid为键，mRNA信息为值。
    # sys.stderr.write('Filter Data...\n')
    dir_needmRNA = {'int':'\t'.join(['1']*8)}
    # 遍历dir_mRNA字典，以向dir_needmRNA字典输入数据，执行判断
    for y in dir_mRNA.keys():
        y_split = dir_mRNA[y].strip().split()
        y_chr_name = y_split[0]
        y_start, y_end, y_score = map(eval,y_split[3:6])
        # 遍历dir_needmRNA字典，若y存在下述的三种情况，则记录入dir_needmRNA
        judge_add = 0
        for z in dir_needmRNA.keys():
            z_split = dir_needmRNA[z].strip().split()
            z_chr_name = z_split[0]
            z_start, z_end, z_score = map(eval,z_split[3:6])
            # 判断染色体名字是否相同。若相同，进行后续判断；若不同，则说明没有该项，记录进dir_needmRNA
            # 因为输入的数据集就是以染色体名称分组的，因此不需要进行该步判断
            # if z_chr_name == y_chr_name:
            # 判断mRNA区间是否有overlap。若有大于100bp的overlap，进行后续判断；若没有，则说明没有该项，记录进dir_needmRNA
            if len(set(range(y_start, y_end)) & set(range(z_start, z_end))) > 100:
                judge_add += 1
                # 判断该条目分值是否比已经记录的有overlap的条目高。若高，则替换；若低，则过滤掉该条目
                if y_score >= z_score:
                    dir_needmRNA.pop(z)
                    dir_needmRNA[y] = dir_mRNA[y]
                else:
                    pass
        # 如果该条目和dir_needmRNA所有项都没有overlap，则需要在加入dir_needmRNA
        if judge_add == 0:
            dir_needmRNA[y] = dir_mRNA[y]
            # else:
            #     dir_needmRNA[y] = dir_mRNA[y]
    dir_needmRNA.pop('int')

    # 过滤低于要求最低数值的mRNA的项
    # 根据geneid从dir_allgene字典内调取mRNA和CDS信息，写入新文件内
    number = 0
    with open(outfile,'a+') as fw:
        for fw_key in dir_needmRNA.keys():
            fw_key_score = float(dir_needmRNA[fw_key].split()[5])
            if fw_key_score > filter_num:
                number += 1
                fw.write(''.join(dir_allgene[fw_key]))
    # sys.stderr.write(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+'\n')
    # sys.stderr.write('Total Lines After Filter:\t' + y_chr_name + '\t' + str(number) + '\n')


# 准备两个数据字典。
# dir_allgene以geneid为键，mRNA信息和CDS信息为值。
# dir_chr_mRNA以contig/scaffold为键，对应的mRNA信息为值，值为列表，列表内为字典
sys.stderr.write(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+'\n')
sys.stderr.write('Reading Data...\n')
dir_allgene = {}
dir_chr_mRNA = {}

for x in open(inputfile):
    if x.split()[2] == 'mRNA':
        q_geneid = x.split('=')[1].split(';')[0]
        chr_name = x.split()[0]
        dir_allgene.setdefault(q_geneid, []).append(x)  # 设置字典值为列表
        # dir_chr_mRNA以每条染色体的名字为键，对应的所有mRNA信息为值。
        # 值是一个集合，集合里嵌套多个字典，键为geneid，值为对应的mRNA信息
        dir_tmp = {}
        dir_tmp[q_geneid] = x
        dir_chr_mRNA.setdefault(chr_name, []).append(dir_tmp)  # 设置字典值为列表，列表的值为新的字典
    elif x.split()[2] == 'CDS':
        q_geneid = x.split('=')[1].split(';')[0]
        dir_allgene.setdefault(q_geneid, []).append(x)  # 设置字典值为列表
    else:
        print(x)

#整理dir_chr_mRNA内的信息，将集合内的多个字典转变为一个字典dir_chrx_mRNA_arrange
if os.path.exists(outfile):
    sys.stderr.write('%s is exist! cleaning it...\n'%outfile)
    os.remove(outfile)

#显示进度条开始
p = progressbar.ProgressBar()
p.start(len(dir_chr_mRNA))
p_number = 0
for key,value in dir_chr_mRNA.items():
    p_number += 1
    p.update(p_number)
    dir_chrx_mRNA_arrange = {}
    chr_name = key
    for x in value:
        new_key, new_value = x.keys()[0], x.values()[0]
        dir_chrx_mRNA_arrange[new_key] = new_value
    #调用过滤函数，过滤后的结果写入新文件内，该文件为追加写入
    filter_write(dir_chrx_mRNA_arrange)
p.finish() #显示进度条结束