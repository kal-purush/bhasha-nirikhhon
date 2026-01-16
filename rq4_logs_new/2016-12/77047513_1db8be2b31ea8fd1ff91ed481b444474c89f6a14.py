genesHUMAN_TAX_ID = "9606"
GENE_INFO_DATA = "/home/rack-shamir2/home/idannuri/Genes/Homo_sapiens.gene_info"
MAP_VIEWER_DATA = "/home/rack-shamir2/home/idannuri/Genes/seq_gene.md"
HEATMAP_TEMPLATE = r"d:\master\GSE18199_binned_heatmaps.zip\GSE18199_binned_heatmaps\HIC_gm06690_chr%s_chr%s_1000000_obs.txt"
SAVED_MATCH_FILE = r"c:\master\match"
DOMAINS_FILE = "/home/rack-shamir2/home/idannuri/domains/hESC/combined/total.combined.domain"
CENTROMERES_FILE = r"c:\master\centromersDic"
TF_FILE = "/home/rack-shamir2/home/idannuri/TF/wgEncodeRegTfbsClusteredV3.bed"
KB_DOMAINS = r"c:\master\domains\KB\kbdomains"
kbdomains = None

import glob

def showFunction(func, smooth = False, smooth_window = 5):
    import matplotlib
    import matplotlib.pyplot as plt
    if smooth:
        func = [sum(func[i:i+smooth_window])*1.0/smooth_window for i in range(len(func)-smooth_window)]
    plt.plot(func)
    plt.show(func)
    return plt

def out(lis):
	for i in lis:
		print i
def pkod(data):
    mifkad = [a.count(chr(i)) for i in range(256)]
    for i in range(256):
        print i," is: ", mifkad[i]

class GeneInfoRecord:
	def __init__(self, line):
		line = lcaline.split("\t")
		self.tax_id = line[0]
		self.GeneID = line[1]
		self.Symbol = line[2]
		self.LocusTag = line[3]
		self.Synonyms = line[4]
		self.dbXrefs = line[5]
		try:
                    self.chromosome = int(line[6])
                except:
                    self.chromosome = line[6]
		self.map_location = line[7]
		self.description = line[8]
		self.type_of_gene = line[9]
		self.Symbol_from_nomenclature_authority = line[10]
		self.Full_name_from_nomenclature_authority = line[11]
		self.Nomenclature_status = line[12]
		self.Other_designations = line[13]
		self.Modification_date = line[14]

        def __str__(self):
                return "%s: Gene_ID is %s, chromosome is %s, map location is %s"%(self.description, self.GeneID, self.chromosome, self. map_location)
                
def createGeneInfo():
    raw = file(GENE_INFO_DATA).read().splitlines()
    DB = [GeneInfoRecord(i) for i in raw[1:]]
    DB = [rec for rec in DB if rec.tax_id == HUMAN_TAX_ID]
    DB = [rec for rec in DB if rec.type_of_gene not in ["other","unknown"]]
    return DB

class MapViewerRecord:
    def __init__(self,line):
        line = line.split("\t")
        self.tax_id = line[0]
        try:
            self.chromosome = int(line[1])
        except:
            self.chromosome = line[1]
        self.start = int(line[2])
        self.end = int(line[3])
        self.chr_orient = line[4]
        self.contig = line[5]
        self.ctg_start = line[6]
        self.ctg_stop = line[7]
        self.ctg_orient = line[8]
        self.feature_name = line[9]
        self.feature_id = line[10]
        self.feature_type = line[11]
        self.group_label = line[12]
        self.transcript = line[13]
        self.evidence_code = line[14]
        if self.feature_id.startswith("GeneID:"):
            self.GeneID = self.feature_id[7:]
        self.GeneInfoMatch = None

    def updateGenesDomains(self, Domains):
        self.Domains = [d for d in Domains if d.chromosome == self.chromosome and d.start <= self.chr_start and d.end >= self.chr_stop]
            
    def __str__(self):
        return "chromosome: %s starts at %s ends at %s"%(self.chromosome, self.start, self.end)
        #self.contig
        #self.ctg_start 
        #self.ctg_stop 
        #self.ctg_orient
        #self.feature_name
        #self.feature_id 
        #self.feature_type
        #self.group_label 
        #self.transcript 
        #self.evidence_code
        
def createMapViewer():
    raw = file(MAP_VIEWER_DATA).read().splitlines()
    DB = [MapViewerRecord(i) for i in raw[1:]]
    DB = [rec for rec in DB if rec.tax_id == HUMAN_TAX_ID]
    DB = [rec for rec in DB if rec.feature_type == "GENE"]
    return DB

def mergeMapInfo(DBGene,DBMap):
    import pickle
    f = file(SAVED_MATCH_FILE, "r")
    match = pickle.load(f)
    for i in range(len(DBMap)):
        if match[i]:
            DBMap[i].GeneInfoMatch = DBGene[match[i]]
    return DBMap
        

def createHICheatmap(index1, index2, obs = True):
    filename = HEATMAP_TEMPLATE%(str(index1),str(index2))
    f = file(filename).read()
    lines = f.splitlines()[1:] # leaving the filename out
    header = lines[0] # columns headers
    rows = lines[1:] # line header + content
    cols = header.split("\t")
    ncols = len([col for col in cols if len(col)>0]) # leaving empty strings out
    nrows = len([row for row in rows if len(row)>0]) # -"-
    heatmap = []
    for irow in range(nrows):
        content = [col for col in rows[irow].split("\t") if len(col) > 0][1:] # leaving header out
        if irow < len(content):
            content[irow] = 0
        heatmap.append(map(lambda x: float(x),content))
    return heatmap        

def intraChromosomeHits(index):
    return map(avg, createHICheatmap(index,index))

class Domain:
    def __init__(self,line):
        line = line.split("\t")
        try:
            self.chromosome = int(line[0][3:])
        except:
            self.chromosome = line[0]
        self.start = int(line[1])
        self.end = int(line[2])

    def __str__(self):
        return "domain in chromosome %s from %d to %d"%(self.chromosome, self.start,self.end)

    def __repr__(self):
        return self.__str__()
    
def createDomains():
    gene2domain = {}
    domainsfn = glob.glob("./domains/gene2domain*")
    for fn in domainsfn:
        key = fn.split("/")[-1][len("gene2domain"):]
        gene2domain[key] = pickle.load(file(fn))
    domain2gene = {}
    domainsfn = glob.glob("./domains/domain2gene*")
    for fn in domainsfn:
        key = fn.split("/")[-1][len("domain2gene"):]
        domain2gene[key] = pickle.load(file(fn))
    domains = {}
    domainsfn = glob.glob("./domains/domains*")
    for fn in domainsfn:
        key = fn.split("/")[-1][len("domains"):]
        if key in domain2gene:
            domains[key] = pickle.load(file(fn))
    """ transforming chromosome to another format"""
    for key in domains:
        for i in range(len(domains[key])):
		try:
			domains[key][i].chromosome = int(domains[key][i].chromosome)
		except:
			if domains[key][i].chromosome == 'X':
				domains[key][i].chromosome = 23
    return domains, gene2domain, domain2gene
    

def avg(lis):
    return sum(lis)*1.0/len(lis)

def findGenesInRange(DB, chr_num, start, stop):
    rv = []
    for rec in DB:
        if rec.chromosome == chr_num and rec.chr_start >= start and rec.chr_stop <= stop:
            rv.append(rec)
    return rv

def updateGenesDomains(DB, Domains):
    for i in range(len(DB)):
        DB[i].updateGenesDomains(Domains)
        if i%10000 == 0:
            print i
    return DB

def load():
    a = createGeneInfo()
    b = createMapViewer()
    print "finished Map Viewer"
    DB = mergeMapInfo(a,b)
    domains = createDomains()
    DB = updateGenesDomains(DB, domains)
    print "finished updating domains"
    import pickle
    centromeres = pickle.load(file(CENTROMERES_FILE,"rb"))
    return DB, domains#, centromeres

def chiscore(mifkad):
    import scipy
    from scipy import stats
    return scipy.stats.chisquare(mifkad)

def argmin(l):
    return l.index(min(l))

"""
HOW TO USE THE CODE FOR FINDING RELATIVE POSITIONS OF GENES IN THE DOMAINS
set(['protein-coding', 'rRNA', 'snRNA', 'tRNA', 'pseudo', 'snoRNA', 'ncRNA'])
a = createGeneInfo()
b = createMapViewer() - takes a long time
DB = mergeMapInfo(a,b)
domains = createDomains()
DB = updateGenesDomains(DB, domains) - takes a long time

"""
def checkRelativeLocation(DB, prefix):
    import scipy
    from scipy import stats
    pc = [d for d in DB if d.GeneInfoMatch and d.GeneInfoMatch.type_of_gene.startswith(prefix)]
    reloc = []
    for gene in pc:
            if gene.Domains:
                    d = gene.Domains[0]
                    reloc.append((gene.chr_start - d.start + (gene.chr_stop-gene.chr_start)/2.)*1.0/(d.end-d.start))
    relocround = map(lambda x: round(x, 2), reloc)
    mifkad = [relocround.count(1.0*i/100) for i in range(100)]
    score = scipy.stats.chisquare(mifkad) # actually comparing to ([sum(mifkad)/100. for i in range(100)])
    return mifkad, score

class CTCF:
    def __init__(self,line):
        line = line.split("\t")
        self.tax_id = line[0]
        try:
            self.chromosome = int(line[4][3:])
        except:
            self.chromosome = line[4]
        self.start = int(line[5])
        self.end = int(line[6])
        self.cell_type = line[10]
    def __str__(self):
        return "CTCF cell type %s in chromosome %s from %d to %d"%(self.cell_type, self.chromosome, self.start,self.end)

    def __repr__(self):
        return self.__str__()

# given a list of numbers, find consecutive ranges within
def findRanges(l):
	ranges = []
	i = 0
	while i < len(l):
		start = l[i]
		while i < len(l)-1 and l[i+1]-l[i] == 1:
			i+=1
		end = l[i]
		ranges.append((start,end))
		i += 1
	return ranges


cell_types = ['HepG2', 'GM12801', 'BJ', 'HeLa', 'AG09309', 'HRPEpiC', 'RPTEC', 'RBC 10 days', 'ProgFib',\
              'ECC-1(DMSO)', 'A549(EtOH)', 'Spleen', 'Gliobla', 'Jurkat', 'HRE', 'AG09319', 'HMF', 'Heart',\
              'NH-A', 'HeLa-S3', 'BoneMarrow', 'RBC 5 days', 'MCF-7(vehicle)', 'WERI-Rb-1', 'NHDF-neo', 'K562',\
              'HCPEpiC', 'HA-sp', 'HL-60', 'MCF-7', 'MEL(DMSO)', 'AG10803', 'ES', 'HBMEC', 'Osteobl', 'HMEC',\
              'HCT-116', 'HEEpiC', 'Kidney', 'A549(DEX)', 'GM19238', 'GM19239', 'HCFaa', 'HEK293', 'CH12',\
              'ES-Bruce4', 'G1E', 'Fibrobl', 'Liver', 'SK-N-SH_RA', 'C2C12', 'NHLF', 'GM06990', 'AoAF', 'CD4+ T',\
              'Cortex', 'GM19240', 'H1-hESC', 'MEF', 'MEL', 'HSMMtube', 'Lung', 'T-47D(DMSO)', 'A549', 'SAEC', 'HUVEC',\
              'AG04449', 'HPAF', 'Dnd41', 'IMR90', 'Caco-2', 'GM12878', 'HVMF', 'GM12875', 'GM12874', 'GM12871', 'GM12870',
              'GM12873', 'GM12872', 'Cerebellum', 'AG04450', 'HSMM', 'NHDF-Ad', 'GM12892', 'GM12891', 'HPF', 'NHEK',\
              'GM12869', 'G1E-ER4(diffProtD)', 'MCF-7(estrogen)', 'GM12864', 'GM12865']


def findCTCFrelativeLoc(celltype, rangenum = 0):
    import pickle

    domains = createDomains()
    typeslocations = pickle.load(file(r"c:\master\types_indices","rb"))
    locs = [t[0] for t in typeslocations if t[1] == celltype]

    f = file(r"c:\master\CTCF\CTCFBSDB_all_exp_sites_Sept12_2012.txt")
    ran = findRanges(locs)[rangenum]
    print ran
    start = ran[0]; end = ran[1]
    for i in range(start):
        a = f.read(1000000)
        del a
    data = f.read(1000000*(end-start+1)).splitlines()[1:-1]
    ctcfs = [CTCF(i) for i in data]
    print len(ctcfs)
    ctcfs = [c for c in ctcfs if c.cell_type == celltype]
    print len(ctcfs)
    
    reloc = []
    for i in range(len(ctcfs)):
        dom = [d for d in domains if d.chromosome == ctcfs[i].chromosome and d.start < ctcfs[i].start and d.end > ctcfs[i].end]
        if dom:
            dom = dom[0]
            reloc.append((ctcfs[i].start - dom.start + (ctcfs[i].end-ctcfs[i].start)/2.)*1.0/(dom.end-dom.start))
        if i%10000 == 0:
            print i
    relocround = map(lambda x: round(x, 2), reloc)
    mifkad = [relocround.count(1.0*i/100) for i in range(100)]
    return mifkad
        

class TF:
    def __init__(self,line):
        line = line.split("\t")
        try:
            self.chromosome = int(line[0][3:])
        except:
            self.chromosome = line[0]
        self.start = int(line[1])
        self.end = int(line[2])
        self.TF = line[3]
    def __str__(self):
        return "transcription factor is %s in chromosome %s from %d to %d"%(self.TF, self.chromosome, self.start,self.end)

    def __repr__(self):
        return self.__str__()

"""
type of transcription factors:
"""
types = ['HSF1', 'SMARCC2', 'SMARCC1', 'POLR3G', 'MAFK', 'CCNT2', 'MAFF', 'ZNF143', 'BHLHE40', 'SIX5', 'HDAC6', 'BCLAF1', 'ZNF263', 'MYC', 'SETDB1',\
'BRCA1', 'TRIM28', 'SP1', 'SP2', 'SP4', 'CREB1', 'ESRRA', 'NR2C2', 'GATA2', 'GATA3', 'GATA1', 'GTF2F1', 'RFX5', 'MAZ', 'RBBP5', 'FOXA1',\
'RUNX3', 'ELK1', 'POU2F2', 'TCF12', 'CTBP2', 'MTA3', 'KDM5A', 'KDM5B', 'FOXP2', 'NFATC1', 'ZNF217', 'PRDM1', 'HMGN3', 'FAM48A', 'GTF3C2',\
'TCF7L2', 'SMARCB1', 'SIRT6', 'CHD1', 'ZKSCAN1', 'CHD2', 'SAP30', 'RDBP', 'IKZF1', 'BACH1', 'EGR1', 'JUN', 'PPARGC1A', 'GTF2B', 'FOXA2', \
'NFE2', 'KAP1', 'ZBTB33', 'ELK4', 'TCF3', 'FOS', 'ZNF274', 'PAX5', 'POLR2A', 'BRF2', 'STAT3', 'BRF1', 'ZEB1', 'TAF7', 'GRp20', 'TAF1',\
'ESR1', 'TBP', 'HNF4G', 'HNF4A', 'MEF2C', 'MEF2A', 'CTCF', 'PHF8', 'HDAC1', 'HDAC2', 'YY1', 'BATF', 'HDAC8', 'RCOR1', 'FOXM1', 'SREBP1',\
'CBX3', 'IRF3', 'SPI1', 'NANOG', 'STAT2', 'STAT1', 'SMC3', 'NR2F2', 'USF2', 'USF1', 'FOSL1', 'FOSL2', 'PML', 'ARID3A', 'TAL1', 'E2F6',\
'BDP1', 'IRF1', 'MAX', 'EP300', 'RELA', 'TBL1XR1', 'IRF4', 'STAT5A', 'GABPA', 'NRF1', 'JUNB', 'BCL11A', 'JUND', 'WRNIP1', 'NFYB', 'NFYA',\
'EZH2', 'ELF1', 'SRF', 'MYBL2', 'SMARCA4', 'TEAD4', 'CEBPB', 'CTCFL', 'TFAP2A', 'TFAP2C', 'UBTF', 'ZZZ3', 'ETS1', 'SUZ12', 'RPC155', 'SIN3A',\
'NR3C1', 'ZBTB7A', 'RAD21', 'REST', 'RXRA', 'POU5F1', 'EBF1', 'PBX3', 'NFIC', 'THAP1', 'E2F4', 'E2F1', 'MXI1', 'CEBPD', 'SIN3AK20', 'MBD4',\
'BCL3', 'ATF1', 'ATF3', 'ATF2']

"""
def createTFdic():
    f = file(TF_FILE).read().splitlines()
    TFs = []
    for i in range(len(f)):
        TFs.append(TF(f[i]))
        if i % 400000 == 0:
            print i
    print "starting making dictionary"
    TFdic = {}
    for t in types:
	TFdic[t] = []
    for tff in TFs:
	TFdic[tff.TF].append(tff)
    return TFdic
"""

def createTFdic():
    import pickle
    TFdic = {}
    for t in types:
        print t,
        TFdic[t] = pickle.load(file("/home/rack-shamir2/home/idannuri/TF/%s"%t,"rb"))
    return TFdic

def findRelativeTF(TFname, resolution = 2, isTF = True, l = None,domains = kbdomains):
    if isTF:
        import pickle
        l = pickle.load(file("/home/rack-shamir2/home/idannuri/TF/%s"%TFname,"rb"))
    reloc = []
    #BUGBUBGUGBUGBGUBGUBGUBGUGBGUBG
    for i in range(len(l)):
            mid = l[i].start + (l[i].end-l[i].start)/2.
            dom = [d for d in domains if str(d.chromosome) == str(l[i].chromosome) and d.start < mid and d.end > mid]
            if dom:
                dom = dom[0]
                reloc.append((mid-dom.start)*1.0/(dom.end-dom.start))
            if i%10000 == 0:
                print i,
    relocround = map(lambda x: round(x, resolution), reloc)
    mifkad = [relocround.count(1.0*i/(10**resolution)) for i in range(10**resolution)]
    import scipy
    #from scipy import stats
    #score = scipy.stats.chisquare(mifkad[3:-3]) # actually comparing to ([sum(mifkad)/100. for i in range(100)])
    return mifkad#, score

def findRelativeTFExcess(TFname, resolution = 2, isTF = True, l = None, excess = 0.25,domains = kbdomains):
    if isTF:
        import pickle
        l = pickle.load(file("/home/rack-shamir2/home/idannuri/TF/%s"%TFname,"rb"))
    reloc = []
    for i in range(len(l)):
            for d in domains:
                if str(d.chromosome) == str(l[i].chromosome):
                    buffer = (d.end - d.start) * excess + (l[i].end-l[i].start)/2.
                    if ((d.start - buffer) < l[i].start) and ((d.end + buffer) > l[i].end):
                        reloc.append((l[i].start - d.start + (l[i].end-l[i].start)/2.)*1.0/(d.end-d.start))
            if i%10000 == 0:
                print i,
    relocround = map(lambda x: round(x+excess, resolution), reloc)
    mifkad = [relocround.count(1.0*i/(10**resolution)) for i in range(10**resolution+int(excess*2*100))]
    import scipy
    #from scipy import stats
    #score = scipy.stats.chisquare(mifkad[3:-3]) # actually comparing to ([sum(mifkad)/100. for i in range(100)])
    return mifkad#, score

def isSymetric(mif):
	first = second = len(mif)/2
	if len(mif) % 2 == 1:
		second +=1
	rv = scipy.stats.chisquare(mif[:first], mif[second:][::-1])
	if rv >  10**(-5):
            return rv
        return 0

def argsort(lis):
    ll = lis[:]
    ll.sort()
    return [lis.index(ll[i]) for i in range(len(ll))]


def isVally(mif, ends = 10, mid = 20, resolution = 1000):
	return avg(mif[:ends]+mif[-1*ends:]) - sum(mif)/resolution > avg(mif[mid:-1*mid]) and (sum(mif) > 100*30)

class HK:
    def __init__(self,name, indices):
        self.name = name
        try:
            self.chromosome = int(indices[0])
        except:
            self.chromosome = indices[0]
        inds = map(int, indices[1:])
        self.start = min(inds)
        self.end = max(inds)
    def __str__(self):
        return "HK name %s in chromosome %s from %d to %d"%(self.name, self.chromosome, self.start,self.end)

    def __repr__(self):
        return self.__str__()

def random_permute(mini, maxi):
    import random
    perm = []
    for i in range(maxi-mini + 1):
        num = random.randint(mini, maxi)
        while num in perm:
            num  = (num+1)%(maxi+1)
        perm.append(num)
    return perm

def rearrange(lis, perm):
    tmp = [0 for i in range(len(lis))]
    for i in range(len(lis)):
        tmp[i] = lis[perm[i]]
    return tmp

def lens(l):
    [len(x) for x in l]

def generateRandomCount(size, num_of_cells = 100):
    import random
    count = [0 for i in range(num_of_cells)]
    for i in range(size):
        ball = random.randint(0, num_of_cells-1)
        count[ball] += 1
    return count

def generateRandomCount2(size, num_of_cells = 100):
    import random
    balls = [random.randint(0,num_of_cells-1) for i in range(size)]
    return [balls.count(i) for i in range(num_of_cells)]

def show3dfunc(func):
    from mpl_toolkits.mplot3d import Axes3D
    import matplotlib.pyplot as plt

    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    n = len(func[0])
    xs = [i for i in range(n) for _ in range(n)]
    ys = range(n) * n
    zs = [a[x][y] for x,y in zip(xs,ys)]

    ax.scatter(xs, ys, zs)

    ax.set_xlabel('X Label')
    ax.set_ylabel('Y Label')
    ax.set_zlabel('Z Label')

    plt.show()
    return plt

def show3dsurface(func):
    import numpy as np
    from mpl_toolkits.mplot3d import Axes3D
    import matplotlib.pyplot as plt

    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    x = np.arange(len(func)-1)
    y = np.arange(len(func[0])-1)
    X, Y = np.meshgrid(x, y)
    zs = np.array([a[x][y] for x,y in zip(np.ravel(X), np.ravel(Y))])
    Z = zs.reshape(X.shape)

    ax.plot_surface(X, Y, Z)

    ax.set_xlabel('X Label')
    ax.set_ylabel('Y Label')
    ax.set_zlabel('Z Label')

    plt.show()

def boundaryTest(domains, dnaElement, space = 100000):
    l = dnaElement[:]
    difs = []
    for i in range(len(l)):
            point = (l[i].start + l[i].end)/2
            dom = [d for d in domains if d.chromosome == l[i].chromosome and min(abs(d.start - point), abs(d.end - point)) < space]
            if dom:
                dom = dom[0]
                difs.append(min(abs(dom.start - point), abs(dom.end - point)))
            if i%10000 == 0:
                print i,
    return difs
    mifkad = [0 for i in range(100)]
    for dif in difs:
        mifkad[dif*100/space] +=1
    #import scipy
    #from scipy import stats
    #score = scipy.stats.chisquare(mifkad[2:-2]) # actually comparing to ([sum(mifkad)/100. for i in range(100)])
    return difs,mifkad#, score

class TSS:
    def __init__(self,line):
        line = line.split()
        try:
            self.chromosome = int(line[0][3:])
        except:
            self.chromosome = line[0][3:]
        self.start = int(line[1])
        self.end = int(line[2])
        self.name = line[3].split("_")[0]

    def __str__(self):
        return "TSS for Gene %s in chromosome %s from %d to %d"%(self.name, self.chromosome, self.start,self.end)

    def __repr__(self):
        return self.__str__()
    
class Gene:
    def __init__(self,line):
        line = line.split()
        try:
            self.chromosome = int(line[3])
        except:
            self.chromosome = 23
        self.start = int(line[4])
        self.end = int(line[5])
        self.MCLID = int(line[0])
        self.name = line[2]
        self.ID = int(line[1])
        self.strand = line[7]
        self.pathway = None
    """def __init__(self,gene, mv):
        self.chromosome = mv.chromosome
        self.start = mv.start
        self.end = mv.end
        self.ID = gene.GeneID"""
    def __str__(self):
        return "Gene ID is %s MCL ID is %s in chromosome %s from %d to %d"%(self.ID, self.MCLID, self.chromosome, self.start,self.end)

    def __repr__(self):
        return self.__str__()

def Genefromstr(line):
    line = line.split()
    newline = ' '.join([line[7],line[3],"unknown",line[10],line[12],line[14],"a","+"])
    return Gene(newline)

class GeneExpression:
    def __init__(self,line):
        fields = line.split("\t")
        position = fields[4]
        chromosome = position.split(":")[0]
        index = position.split(":")[1]
        try:
            self.chromosome = int(chromosome[3:])
        except:
            self.chromosome = 23
        self.start = int(index.split("-")[0])
        self.end = int(index.split("-")[1])
        self.ID = int(fields[0])
        self.data = line
        self.HeLa = sum(map(lambda x: float("0"+x), fields[85:87]))/3
        self.GM12878 = sum(map(lambda x: float("0"+x), fields[107:109]))/3
        self.NHEK = sum(map(lambda x: float("0"+x), fields[91:93]))/3
        self.IMR90 = sum(map(lambda x: float("0"+x), fields[101:103]))/3
        self.HUVEC = sum(map(lambda x: float("0"+x), fields[98:100]))/3
    def __str__(self):
        return "Gene ID is %s in chromosome %s from %d to %d"%(self.ID, self.chromosome, self.start,self.end)

    def __repr__(self):
        return self.__str__()

class tRNA:
    def __init__(self,line):
        line = line.split()
        try:
            self.chromosome = int(line[0][3:])
        except:
            self.chromosome = line[0][3:]
        self.start = int(line[1])
        self.end = int(line[2])
    def __str__(self):
        return "trna in chromosome %s from %d to %d"%(self.chromosome, self.start,self.end)

    def __repr__(self):
        return self.__str__()

class histone(tRNA):
    def __init__(self,line):
        tRNA.__init__(self,line)

    def __repr__(self):
        return tRNA.__repr__

class lncRNA(tRNA):
    def __init__(self,line):
        tRNA.__init__(self,line)

    def __repr__(self):
        return "lncRNA in chromosome %s from %d to %d"%(self.chromosome, self.start,self.end)

class Group:
    def __init__(self,line):
        line = line.split("\t")
        self.name = line[0]
        self.indices = map(int,line[1].split(";"))
    def __str__(self):
        return "Group name is %s indices are %s"%(self.name, self.indices)

    def __repr__(self):
        return self.__str__()

def middle(gene):
    return gene.start + (gene.end - gene.start)/2

def InDomain(d,gene):
    mid = gene.start + (gene.end-gene.start)/2.
    if d.chromosome == 'X':
        d.chromosome = '23'
    return str(d.chromosome) == str(gene.chromosome) and (d.start < mid and d.end > mid)

def check(genes_kegg, maxdis = 300*1000, mindis = 0):
    import itertools 
    dg = []; db = []; ds = []; dn = []; 
    #WARNING
    dgg = []; dss = []
    good = 0; bad = 0; seperate = 0; none = 0
    for gr in genes_kegg:
            for x in itertools.product(gr, repeat = 2):
                    if x[0].chromosome != x[1].chromosome:
                            continue 
                    if x[0] == x[1]:
                            continue
                    if (abs(middle(x[0]) - middle(x[1])) > maxdis) or (abs(middle(x[0]) - middle(x[1])) < mindis):
                            continue
                    dis = abs(middle(x[0]) - middle(x[1]))
                    domains0 = [d for d in kbdomains if InDomain(d,x[0])]
                    domains1 = [d for d in kbdomains if InDomain(d,x[1])]
                    domainss = [d for d in domains0 if InDomain(d,x[1])]
                    if domainss:
                            good += 1
                            dg.append(dis)
                            #WARNING
                            dgg.append([x[0],x[1],domainss])
                            continue
                    if domains0 and domains1:
                            seperate += 1
                            ds.append(dis)
                            dss.append([x[0],x[1]])
                            continue
                    if not domains0 and not domains1:
                            none += 1
                            dn.append(dis)
                            continue
                    bad +=1
                    db.append(dis)
    #WARNING
    return dgg,dss
    return list(set(dg)),list(set(db)),list(set(ds)),list(set(dn))

def rand_test(x, dis = [(0, 50*1000),(0, 100*1000),(0, 150*1000),(0, 200*1000),(0, 250*1000),(0, 300*1000),(0, 350*1000)], end = ""):
    f = file("./random_file_domains_complex" + end,"wb")
    sum_genes = pickle.load(file("./Genes/unique_genes"))
    from random import randint
    for i in range(x):
        #indices = [[randint(1,len(sum_genes)-1) for i in range(len(gr))] for gr in genes_kegg]
        #rand_genes = [[sum_genes[i] for i in gr] for gr in indices]
        rand_genes = [[genes[int(g.chromosome)-1][randint(0,len(genes[int(g.chromosome)-1])-1)] for g in gr] for gr in genes_kegg]
        result = []    
        for d in dis:
            a = map(len, check(rand_genes,d[1],d[0]))
            result.append(a[0]*1.0/sum(a))
        print result
        f.write(str(result) + "\n")
        f.close()
        f = file("./random_file_domains_complex" + end,"a")
    f.close()

def sum_diffs(genes):
    sum = 0
    mids = [(g.end + g.start)/2 for g in genes]
    for a in mids:
      for b in mids:
          sum += abs(a-b)
    return sum

def rand_test_dist(x, dis = [(0, 50*1000),(0, 100*1000),(0, 150*1000),(0, 200*1000),(0, 250*1000),(0, 300*1000),(0, 350*1000)], end = ""):
    f = file("./random_file_domains_complex" + end,"wb")
    sum_genes = pickle.load(file("./Genes/unique_genes"))
    from random import randint
    sum_diff_genes = []
    for i in range(len(genes_kegg)):
        sum_diff_genes.append(sum_diffs(genes_kegg[i]))
    for i in range(x):
        #indices = [[randint(1,len(sum_genes)-1) for i in range(len(gr))] for gr in genes_kegg]
        #rand_genes = [[sum_genes[i] for i in gr] for gr in indices]
        rand_genes = []
        for k in range(len(genes_kegg)):
            gr = genes_kegg[k]
            diffs = sum_diff_genes[k]
            temp = [genes[int(g.chromosome)-1][randint(0,len(genes[int(g.chromosome)-1])-1)] for g in gr]
            while sum_diffs(temp) > diffs:
                temp = [genes[int(g.chromosome)-1][randint(0,len(genes[int(g.chromosome)-1])-1)] for g in gr]
            rand_genes.append(temp)
        result = []    
        for d in dis:
            a = map(len, check(rand_genes,d[1],d[0]))
            result.append(a[0]*1.0/sum(a))
        print result
        f.write(str(result) + "\n")
        f.close()
        f = file("./random_file_domains_complex" + end,"a")
    f.close()

def parse_results(filename):
    f = file(filename,"rb").read().splitlines()
    results = []
    for line in f:
        values = map(float, line.replace("[","").replace("]","").split(", "))
        results.append(values)
    return results
    
def check_relative_dists(genes, max_dis = 500*1000):
    dgg = check(genes, 500*1000)[0]
    dists = [abs(middle(dg[0])- middle(dg[1])) for dg in dgg]
    lens = [dg[2][0].end - dg[2][0].start for dg in dgg]
    rets = list(set([dists[i]*1.0 / lens[i] for i in range(len(dgg))]))
    rets = [int(ret*100) for ret in rets]
    mif = [rets.count(i) for i in range(100)]
    return rets, mif, lens

def comp_pair(p1,p2):
    return (((p1[0] == p2[0]) and (p1[1] == p2[1])) \
            or ((p1[1] == p2[0]) and (p1[0] == p2[1])))

def set_pairs(pairs):
    temp = {}
    for pair in pairs:
        if pair[0] not in temp:
            if pair[1] not in temp:
                temp[pair[0]] = [pair[1]]
            elif pair[0] not in temp[pair[1]]:
                temp[pair[1]].append(pair[0])
        elif pair[1] not in temp[pair[0]]:
            temp[pair[0]].append(pair[1])
    
    result = []
    for key in temp.keys():
        for v in temp[key]:
            if key!=v:
                result.append([key,v])
    return result
    
def create_domain_genes_dic(genes):
    dgg = find_cofunc_pairs_inDomain(genes, 1000*1000, ret = 1)
    d = {}
    for dg in dgg:
        if d.has_key(dg[2]):
            d[dg[2]].append(dg[:2])
        else:
            d[dg[2]] = [dg[:2]]
    for k in d.keys():
        d[k] = set_pairs(d[k])
    return d[k]

def pair_dis(pair):
    return abs(middle(pair[0])-middle(pair[1]))

def sum_list(lists):
    return reduce(lambda x,y:x+y, lists)

class Loop:
    def __init__(self,line):
        line = line.split(",")
        try:
            self.chromosome = int(line[0][3:])
        except:
            self.chromosome = 23
        self.lstart = int(line[1])
        self.lend = int(line[2])
        self.rstart = int(line[4])
        self.rend = int(line[5])
        self.start = self.lstart
        self.end = self.rend

    def __str__(self):
        return "Loop chromosome %d from %d,%d to %d,%d"\
               %(self.chromosome, self.lstart,self.lend,self.rstart,self.rend)

    def __repr__(self):
        return self.__str__()

"""
TIUD:
- kegg_groups is the list of gene groups (indices) according to kegg file
- kegg_gene_groups is the list of gene groups (Gene class) accordding to kegg file, after merging duplicate genes!
-
"""

import os
import pickle
try:
    try:
        a = domains["NHEK"]
    except:
        import rlcompleter, readline
        readline.parse_and_bind('tab:complete')
        os.chdir("/home/gaga/idannuri")
        #allg = pickle.load(file("./Genes/unique_genes"))
        #genes = [[g for g in allg if str(g.chromosome) == str(i)] for i in range(1,24)]
        genes = pickle.load(file("./FinalData/MCLBased_Genes"))
        domains, gene2domain, domain2gene = createDomains()
        genes_kegg = pickle.load(file("./Genes/kegg_genes_groups"))
        new_genes_kegg = pickle.load(file("./Genes/kegg_final_version"))
        #new_genes = pickle.load(file("./Genes/new_unique_genes"))
        #new_kegg = pickle.load(file("./GeneExp/kegg_geneExpression_groups"))
        genes_corum = pickle.load(file("./Genes/corum_genes_groups"))
        genes_intact = pickle.load(file("./Genes/intact_genes_groups"))
        Adomains = pickle.load(file("./domains/As.list"))
        Bdomains = pickle.load(file("./domains/Bs.list"))
        kbAdomains = pickle.load(file("./subcompartments/kbAdomains"))
        kbBdomains = pickle.load(file("./subcompartments/kbBdomains"))
        genes_reactome = pickle.load(file("./Pathway/genes_reactome"))
        gene2domainGM = pickle.load(file("./domains/gene2domainGM"))
        kegg = pickle.load(file("./FinalData/MCLBased_newKEGGPathways_withoutOlfactory"))
        inter_intra_domains = pickle.load(file("./domains/inter_intra_domains"))
        hetero = pickle.load(file("./heterochromatin/hetero"))
    execfile("./Python/tests.py")
except:
    os.chdir(r"R:\home\idannuri")
    genes = pickle.load(file(r".\Genes\unique_genes"))
    genes = [[g for g in genes if str(g.chromosome) == str(i)] for i in range(1,24)]
    domains = pickle.load(file(r".\domains\KB\kbdomains"))
    genes_kegg = pickle.load(file(r".\Genes\kegg_genes_groups"))
    genes_corum = pickle.load(file(r".\Genes\corum_genes_groups"))
    genes_intact = pickle.load(file(r".\Genes\intact_genes_groups"))
kbdomains = domains


