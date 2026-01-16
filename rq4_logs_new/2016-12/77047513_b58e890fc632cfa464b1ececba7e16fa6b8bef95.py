import numpy

class Coordinate:
    def __init__(self, line):
        line = line.split()
        self.i = int(line[0])
        self.j = int(line[1])
        self.Mij = float(line[2])

    def __str__(self):
        return "%s %s %s\n " % (self.i, self.j, self.Mij)

    def __repr__(self):
        return self.__str__()

def matrixFromFile(filename):
    import numpy
    data = file(filename,"rb").readlines()
    result = numpy.zeros((len(data)-2,len(data)-2))
    for i in range(2,len(data)):
        vals = map(float,data[i].split()[1:])
        for j in range(len(vals)):
            result[i-2,j]=vals[j]
    return result

def matrix2contracted(matrix, badrows, badcols):
    ##This gets rid of rows and columns whose value is zero. But watch out, it doesn't copy the original matrix (for speed) but can muck it up as a result.

    rows, cols = matrix.shape
    contracted = matrix[:rows - len(badrows), :cols - len(badcols)]
    goodrows = [i for i in range(rows) if i not in badrows]
    goodcols = [i for i in range(cols) if i not in badcols]
    for i in range(len(goodrows)):
        for j in range(len(goodcols)):
            contracted[i, j] = matrix[goodrows[i], goodcols[j]]

    return contracted

def vector2expanded(vector,badrows):
##This restores rows and columns removed by matrix2contracted into the eigenvector.

        from numpy import zeros
        newvector=zeros(len(vector)+len(badrows),dtype=float)
        goodrows=[i for i in range(len(vector)+len(badrows)) if i not in badrows]
        for i in range(len(goodrows)):
                newvector[goodrows[i]]=vector[i]

        return newvector


def matrix2expanded(matrix,badrows):
##This restores rows and columns removed by matrix2contracted into the eigenvector.

        from numpy import zeros
        dim = len(vector)+len(badrows)
        newmatrix=zeros((dim,dim), dtype=float)
        goodrows=[i for i in range(len(vector)+len(badrows)) if i not in badrows]
        for i in range(len(goodrows)):
            for j in range(len(goodrows)):
                newvector[goodrows[i],goodrows[j]]=matrix[i,j]
        return newmatrix

#################################################
def matrix2zeroindex(matrix):
    rows, cols = matrix.shape
    zerorows = []
    zerocols = []
    for i in range(rows):
        if cols == len([1 for j in range(cols) if matrix[i, j] == 0]):
            zerorows.append(i)
    for j in range(cols):
        if rows == len([1 for i in range(rows) if matrix[i, j] == 0]):
            zerocols.append(j)

    return zerorows, zerocols

"""
the function gets the obs\exp normed Hi-C matrix and
returns the PCA results and the correlation matrix """
def ABcompartments(normmatrix):
    from numpy import corrcoef
    from scipy import cov
    from scipy import linalg
    from numpy import matrix
    from copy import deepcopy

    badrows, badcols = matrix2zeroindex(normmatrix)
    newnormmatrix = matrix2contracted(normmatrix, badrows, badcols)
    corrmatrix = corrcoef(newnormmatrix)
    means = numpy.matrix([numpy.ones(len(corrmatrix[row]))*(float(sum(corrmatrix[row, :])) / corrmatrix.shape[0])\
                          for row in range(len(corrmatrix))])
    copymatrix = deepcopy(corrmatrix) - means
    print "calculating eigvecs"
    covmatrix = cov(copymatrix)
    evals, eigs = linalg.eig(covmatrix)
    evreal = [eval.real for eval in evals]
    #eigs = [vector2expanded(eigs[:,i],badrows) for i in range(len(eigs))]
    return evreal, eigs, copymatrix
