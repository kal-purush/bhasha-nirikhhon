# coding=utf-8
"""
This modlue is for decoding  Gff3 file or gtf file
"""
import sys

geneDict = {}
genomeDict = {}


class Gene:
    """
    用来存储基因的信息，包括基因名，scaffold,以及起始和终止位点
    """

    def __init__(self, listitems, genename):
        """init values"""
        assert isinstance(listitems, list)
        assert isinstance(genename, str)
        self.scaffold = listitems[0]
        self.start = int(listitems[3])
        self.end = int(listitems[4])
        self.genename = genename


def classifyitems(listitems):
    if listitems[2] == 'gene':
        tmp = listitems[8].split(';')
        genename = tmp[2].split(' ')[2].replace('"', '')
        tmpgene = Gene(listitems[0:5], genename)
        if genomeDict.has_key(tmpgene.scaffold):
            genomeDict[tmpgene.scaffold].append(tmpgene)
        else:
            genomeDict[tmpgene.scaffold] = [tmpgene]


def B_Search(length, pos, scaffold): #二分搜索法
    start, end = 0, length
    cursor = (start + end) // 2
    while start != cursor:
        if genomeDict[scaffold][cursor].start > pos:
            end = cursor
        else:
            start = cursor
        cursor = (start + end) // 2
    return cursor


def GffPatternDet(start, end, rstart, rend):
    IsContinue = False
    IsOver = 0
    Overlab = False
    if end >= rend:
        IsContinue = True
        if start >= rend:  # gs<=ge<=as<=ae
            pass
        elif rstart <= start < rend:  # gs<=as<=ge<=ae
            Overlab = True
        else:  # as<=gs<=ge<=ae
            Overlab = True
            IsOver = 1
    elif end <= rstart:  # as<=ae<=gs<=ge
        pass
    elif start <= rstart:  # as<=gs<=ae<=ge
        Overlab = True
    else:  # gs<=as<=ae<=ge
        Overlab = True
    return IsContinue, Overlab, IsOver


# 如果是返回
def IsFullLength(scaffold, start, end):
    genelist = []
    fulllen = []
    IsContinue = True
    if genomeDict.has_key(scaffold):
        lens = len(genomeDict[scaffold])
        idx = B_Search(lens, start, scaffold)
        while IsContinue:
            IsContinue, IsOverlap, IsOverAll = GffPatternDet(start, end, genomeDict[scaffold][idx].start,
                                                             genomeDict[scaffold][idx].end)
            if IsOverAll or IsOverlap:
                genelist.append(genomeDict[scaffold][idx].genename)
                fulllen.append(IsOverAll)
            idx += 1
        if len(genelist)==0:
            genelist.append("intergenic")
            fulllen.append(0)
        return genelist,fulllen
    else:
        raise ValueError("%s scaffold information not in Gff file!",scaffold)


def decodegff(gtffilename):
    """

    :rtype: str
    """
    with open(gtffilename) as gtffile:
        for item in gtffile:
            if item.find("#!") != 0:
                break
        for item in gtffile:
            listitems = item.split("\t")
            classifyitems(listitems)
    for key, val in genomeDict.items():
        assert isinstance(val, list)
        val.sort(key=lambda x: x.start)  # 按照起始位点排序


def main():
    print("This is a Test!")


if __name__ == '__main__':
    sys.exit(main())