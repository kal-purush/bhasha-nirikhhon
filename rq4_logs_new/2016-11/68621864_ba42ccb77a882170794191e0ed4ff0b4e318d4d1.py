#!/usr/bin/env python
from __future__ import division
import os
import argparse
import glob



#proportions based on 14 pat BAMs

#class creates object

class QueryManager:
	storageDir=None	
	def setStorage(self, storageDir):
		self.storageDir=storageDir
	
	#sets the function 'query' and describes what imputs are needed	
	def query(self, geneListFile, numberOfSamples, outfilename):
		outfilename2=outfilename+"_depth"
		#loads a genelist from the geneListFile provided			
		genelist=self.loadgenelist(geneListFile)
		#grab relevent files from input specified in command line
		percentCoverFilenames=glob.glob ("%s/*.%s.averageCovPercentage.txt"%(self.storageDir, numberOfSamples))
		#prepare dictionary of coverage percentages (20x)		
		geneCoveragePercentages={}
		for gene in genelist:
			geneCoveragePercentages[gene]=[] 
		#search for gene coverages in all files	
		for filename in percentCoverFilenames:
			for line in open(filename):
				line=line.strip()
				line=line.split("\t")
				genename=line[0]
				coveragePerc=float(line[1])
				
				if genename in genelist:
					geneCoveragePercentages[genename].append(coveragePerc)
		#print geneCoveragePercentages 
		#take average coverage from across all GIAB files	
		averageCoveragePercentage={}	
		for gene in genelist:
			coveragelist=geneCoveragePercentages[gene]
			if len(coveragelist) > 0 :
				averagecoverage=sum(coveragelist)/len(coveragelist)
				averageCoveragePercentage[gene]=averagecoverage
			else :
				averageCoveragePercentage[gene]=None
		#output dictionary to file
		outfile=open(outfilename+"_20xcoverage", "w")
		for gene in averageCoveragePercentage:
			outfile.write("%s\t%s\n"%(gene, averageCoveragePercentage[gene])) 
		outfile.close()



		#get relevent depth of coverage files
		coverageDepthFilenames=glob.glob ("%s/*.%s.averageCovDepth.txt"%(self.storageDir, numberOfSamples))
		#prepare dictionary of coverage depths
		geneCoverageDepth={}
		for gene in genelist:
			geneCoverageDepth[gene]=[]
		for filename in coverageDepthFilenames:
			for line in open(filename):
				line=line.strip()
				line=line.split("\t")
				genename=line[0]
				coverageDepth=float(line[1])	
				if genename in genelist:
					geneCoverageDepth[genename].append(coverageDepth)
		#print geneCoverageDepth
		averageCoverageDepth={}
		for gene in genelist:
			depthlist=geneCoverageDepth[gene]
			if len(depthlist) > 0 :			
				averageDepth=sum(depthlist)/len(depthlist)
				averageCoverageDepth[gene]=averageDepth
			else :
				averageCoverageDepth[gene]=None
		outfile2=open(outfilename2, "w")
		for gene in averageCoverageDepth:
			outfile2.write("%s\t%s\n"%(gene, averageCoverageDepth[gene])) 
		outfile.close()	



	def loadgenelist(self, geneListFile):
		genelist=[]		
		for line in open(geneListFile):
			line=line.strip()
			genename=line
			if genename=='':
				continue
			genelist.append(genename) 
		return genelist


class InputManager:
	mincoverage=20
			

	storageDir=None	
	def setStorage(self, storageDir):
		self.storageDir=storageDir
		if not os.path.exists(storageDir):
			print 'Creating storage directory'
			os.mkdir(storageDir)

			
	subsamples=[14,15,16]	
	def subSample(self, bam):
		#subsample the bam file, remember name of subsampled bams
		for subsample in self.subsamples:
			proportion=14.0/subsample
			#create output name
			outprefix=bam.replace(".bam", ".%s" %subsample)			
			#don't bother downsampleing if proportion=100%
			if proportion==1.0:
				cmd="ln -s %s %s.bam" %(bam, outprefix)
			else:
				#cmd="java -jar ~/bin/picard-tools-1.119/DownsampleSam.jar I=%s O=%s P=%.5f" %(bam, outname, proportion)
				cmd="samtools view -s %.5f -b -h %s > %s.bam" %(proportion, bam, outprefix)				
			print cmd
			os.system(cmd) #DEBUG
			
			#index the bam
			cmd="samtools index %s.bam" %(outprefix)
			print cmd
			os.system(cmd)  #DEBUG
			
			#write like command above to run from terminal
			cmd="genomeCoverageBed -bga -ibam %s.bam >  %s.genomecoverage.bed" %(outprefix, outprefix)
			print cmd
			os.system(cmd)
			cmd="intersectBed -wb -a %s.genomecoverage.bed -b AgilentQXTGenes.bed > %s.genecov.bed" %(outprefix, outprefix) 
			print cmd
			os.system(cmd)

			self.extractCoverageDepth ("%s.genecov.bed"%(outprefix), "%s/%s.averageCovDepth.txt"%(self.storageDir, outprefix))
			self.extractCoveredProportion ("%s.genecov.bed"%(outprefix), "%s/%s.averageCovPercentage.txt"%(self.storageDir, outprefix))			


	
	#get coverage depth from BED - create dict
	def extractCoverageDepth (self, covbed, outfilename):
		'''covbed=intersected coverage bedfile eg %s.genecov.bed'''
		coveragedict = {}
		genelengthdict = {} #{GENENAME : 152637bp}
		for line in open(covbed):
			line=line.strip()
			field=line.split('\t') 
			#['1', '169680651', '169680682', '3', '1', '169680526', '169680707', 'SELL']
			geneName=field[7]
			startPosition=int(field[1])
			endPosition=int(field[2])
			coverage=float(field[3])
			#store region length
			regionlength=endPosition-startPosition
			#check if gene is in dictionary
			if geneName not in genelengthdict:
				genelengthdict[geneName]=0
			genelengthdict[geneName]=genelengthdict[geneName]+regionlength
			#store total coverage
			totalCoverage=regionlength*coverage
			if geneName not in coveragedict:
				coveragedict[geneName]=0.0
			coveragedict[geneName]=coveragedict[geneName]+totalCoverage
		#calculate average coverage per gene
		geneList=coveragedict.keys()
		genecoverage= {}
		for gene in geneList:
			genecoverage[gene]=coveragedict[gene]/genelengthdict[gene]
		#export to file	
		outfile=open(outfilename, "w")
		for gene in geneList:
			outfile.write("%s\t%s\n"%(gene, genecoverage[gene])) 	

		outfile.close()	



	def extractCoveredProportion (self, covbed, outfilename):
		'''covbed=intersected coverage bedfile eg %s.genecov.bed'''
		coveredbasesdict = {}
		genelengthdict = {} #{GENENAME : 152637bp}
		for line in open(covbed):
			line=line.strip()
			field=line.split('\t') 
			#['1', '169680651', '169680682', '3', '1', '169680526', '169680707', 'SELL']
			geneName=field[7]
			startPosition=int(field[1])
			endPosition=int(field[2])
			coverage=float(field[3])
			#store region length
			regionlength=endPosition-startPosition
			#check if gene is in dictionary
			if geneName not in genelengthdict:
				genelengthdict[geneName]=0
			genelengthdict[geneName]=genelengthdict[geneName]+regionlength
			#add region length if covered			
			if geneName not in coveredbasesdict:
				coveredbasesdict[geneName]=0.0
			if coverage >= self.mincoverage:
				coveredbasesdict[geneName]=coveredbasesdict[geneName]+regionlength
			
		#calculate average coverage per gene
		geneList=coveredbasesdict.keys()
		genecoveragepercent = {}
		for gene in geneList:
			genecoveragepercent[gene]=(coveredbasesdict[gene]/genelengthdict[gene])*100.0
		#export to file	
		outfile=open(outfilename, "w")
		for gene in geneList:
			outfile.write("%s\t%s\n"%(gene, genecoveragepercent[gene])) 	

		outfile.close()	



#coverage['s%']=     %(genes)#dont think this is possible - genes from virtual panel
	

parser = argparse.ArgumentParser(description='loads BAM file and generates average coverage')
parser.add_argument('--bam', help='input bam file based on 14 patients per run')
parser.add_argument('--storagedir', help='directory for storing average coverages', default='./average_gene_coverages/')
parser.add_argument('--geneListFile', help='gene list for virtual panel')
parser.add_argument('--numberOfSamples', help='number of samples on the run', type=int)
parser.add_argument('--outfilename', help='name of file for query output eg breadth of storage')
args = parser.parse_args()


if args.bam != None :
	inputmanager=InputManager()
	inputmanager.setStorage(args.storagedir)
	inputmanager.subSample(args.bam)

if args.geneListFile != None :
	querymanager=QueryManager()
	querymanager.setStorage(args.storagedir)
	querymanager.query(args.geneListFile, args.numberOfSamples, args.outfilename)