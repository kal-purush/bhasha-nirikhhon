
def unpickle(file):
    import pickle
    with open(file, 'rb') as fo:
        dict = pickle.load(fo)
    return dict


dict = unpickle('/Users/ingridchristensen/PycharmProjects/ImagesNN/files/cifar-10-batches-py/data_batch_1')
print (dict)