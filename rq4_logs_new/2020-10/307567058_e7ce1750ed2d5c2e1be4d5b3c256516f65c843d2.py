import string,sys,numpy,glob

fp=glob.glob('*_cms3.txt')
for fname in fp:
	fp1=open(fname,'r')
	CMS1=[]	#value initialization
	CMS2=[]
	CMS3=[]
	CMS4=[]
	gene = fname.split('_cms3.txt')[0]
	hd=fp1.readline()
	for line in fp1:
		line_temp=line[:-1].split('\t')
		if 'CMS1' == line_temp[0]:
			CMS1.append(float(line_temp[1]))
		elif 'CMS2' == line_temp[0]:
			CMS2.append(float(line_temp[1]))
		elif 'CMS3' == line_temp[0]:
			CMS3.append(float(line_temp[1]))
		elif 'CMS4' == line_temp[0]:
			CMS4.append(float(line_temp[1]))
#	print TPM
#	print TPM_N

	X = numpy.array(CMS1)
	Z = numpy.median(X)
	Y = numpy.min(X)
	K = numpy.max(X)
	print gene
	print  'CMS1 : median = ', Z, '	max = ', K
#	print  'CMS1 : min = ', Y

	a = numpy.array(CMS2)
	b = numpy.median(a)
	d = numpy.max(a)
	print 'CMS2 : median = ', b, '	max = ', d

	e = numpy.array(CMS3)
	f = numpy.median(e)
	g = numpy.max(e)
	print 'CMS3 : median = ', f, '	max = ', g

	h = numpy.array(CMS4)
	i = numpy.median(h)
	j = numpy.max(h)
	print 'CMS4 : median = ', i, '	max = ', j
	print "\n"