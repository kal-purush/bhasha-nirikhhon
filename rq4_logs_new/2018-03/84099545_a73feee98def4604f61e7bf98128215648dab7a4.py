import numpy as np
from gensim.models.keyedvectors import KeyedVectors

class input_to_embeddings:
    '''
    This class contains all the methods that take the extracted lists of information from conllU data and converts
    those information into matrices
    '''

    '''
    This method takes a list of lists of information for each word from inputdata as input and creates a matrix
    with looked up embeddings as output
    inputlist: a list of lists 
    can either contain semantic information, morphological or syntactical for examle 
    [Der, start1, start2, Haupteingang, ist]
    word1 kontextL1 kontextL2 kontextR1 kontextR2
    
    if we would do that for pos tags one list in the total list would look like that:
    [DET, start1, start2, NOM, VB]
    
    inputsize: the total number of instances in the data
    embeddingdimension: the dimension of the embeddings (most of the time this is 100)
    embeddingtable: the corresponding lookupmodel for (pos/morph)embeddings
    '''
    def create_input(inputsize, embeddingdimension, inputlist, embeddingtable, unknowndict):
        inputmatrix = np.zeros(shape=(inputsize, embeddingdimension*len(inputlist[0])), dtype=np.float32)
        for instance in range(inputsize):
            concatembedding = np.empty(shape=0)
            for el in inputlist[instance]:
                if el in embeddingtable:
                    embedding = embeddingtable[el]
                elif el == "start1":
                    embedding = unknowndict["start1"]
                elif el == "start2":
                    embedding = unknowndict["start2"]
                elif el == "end1":
                    embedding = unknowndict["end1"]
                elif el == "end2":
                    embedding = unknowndict["end2"]
                else:
                    embedding = unknowndict["unknown"]
                concatembedding = np.concatenate((concatembedding,embedding))
            inputmatrix[instance] = concatembedding
        return inputmatrix

    '''
    converts the list of labels and the list of relative ranges between word pairs into numpy matrix
    '''
    def get_oneDim_matrix(inputlist):
        matrix = np.array(inputlist, dtype=np.float32)
        return matrix

    '''
    Read pretrained word embeddings from a binary file
    '''
    def read_word_embeddings(embed_file):
        word_vectors = KeyedVectors.load_word2vec_format(embed_file, binary=True)
        return word_vectors


    """
    This method creates a small dictionary for the 'special' embedding which are:
    start1: if the first left kontext is missing
    start2: if the second left kontext is missing
    end1: if the first right kontext is missing
    end2: if the second right kontext is missing
    unknown: if a word/pos/morph tag is not in the embeddingdictionary
    """
    def create_unknownembeddings(dimension):
        start1 = np.zeros(shape=dimension)
        start2 = np.zeros(shape=dimension)
        end1 = np.zeros(shape=dimension)
        end2 = np.zeros(shape=dimension)
        start1[0] = 1.0
        start2[1] = 1.0
        end1[dimension-2] = 1.0
        end2[dimension-1] = 1.0
        unknown = np.random.sample(dimension)
        unknownembeddings_dict = {}
        unknownembeddings_dict["start1"] = start1
        unknownembeddings_dict["start2"] = start2
        unknownembeddings_dict["end1"] = end1
        unknownembeddings_dict["end2"] = end2
        unknownembeddings_dict["unknown"] = unknown
        return  unknownembeddings_dict


    '''
    Saves a matrix as pickle object
    '''
    def save_as_pickle(matrix, path):
        np.save(path, matrix)

if __name__ == '__main__':
    embedding_file = '/home/neele/Dokumente/DeepLearning/wikipedia-100-mincount-30-window-8-cbow.bin'
    un = input_to_embeddings.create_unknownembeddings(100)
    print(un["start1"])
    #embeddings = read_word_embeddings(embedding_file)
    #embeddings.syn
    #convert_word_embeddings(embeddings)