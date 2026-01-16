#K-Nearest Neighbor Implementation Project
#Alexander Alvarez

#class for storing the data sets
class dataset:
    total_set = []
    training_set = [[]]
    test_set = [[]]

    def __init__(self,file_name):
        total_set = []
        #open input and output files
        with open(file_name) as readIn:
            #iterate over each line in input file
            for line in readIn:
                features = line.split(",")
                total_set.append(features)
        self.total_set = total_set
        print(total_set)

    def k_split(k):
        test_size = len(total_set.length)
        training_set = [[]]
        test_set = [[]]

        for i in range(0, k*test_size):
            training_set.append(total_set[i])
        for i in range(k*test_size, (k+1)*test_size):
            test_set.append(total_set[i])
        for i in range((k+1)*test_size, len(total_set)):
            training_set.append(total_set[i])

#class containing methods for preprocessing the datasets
class pre_processing:
    def remove_headers(data):
        del data.total_set[0]

#class containing methods implementing the K-NN algorithms
class k_nearest_neighbor:
    def k_nearest(data, k):
        dataset.k_split(k)

#class for driving the program
class main:

    abalone = dataset("data/abalone.data")
    car = dataset("data/car.data")
    forest_fires = dataset("data/forestfires.csv")
    machine = dataset("data/machine.data")
    segmentation = dataset("data/segmentation.data")
    wine_red = dataset("data/winequality-red.csv")
    wine_white = dataset("data/winequality-white.csv")

    #def run():


driver = main()
#main.run()