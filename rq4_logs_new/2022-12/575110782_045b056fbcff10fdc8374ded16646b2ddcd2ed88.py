import sys
import os
import argparse
import json
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import normalize
from sklearn.linear_model import Perceptron
from MLPmodel.mlp import MLP

BEATSAMPLES = 100

def getArgParser():
    desc =  'This driver runs a genre classification Multi Level Perceptron neural netwrok\n\n'
    desc += 'A preprocessiong of mp3 files to json files is required' 
    # TODO add more

    parser = argparse.ArgumentParser(
        prog = 'genreClassificationDriver: MLP genre classification',
        description = desc,
        epilog = "" # TODO
    )
    parser.add_argument('--data', type=str, help='folder containing database of json files', required=True)
    parser.add_argument('--lr', type=float, help='learning rate', required=False, default=0.1)
    parser.add_argument('--epochs', type=int, help='maximum number of epochs', required=False, default=100)
    parser.add_argument('--hiddenLayers', type=int, help='number of hidden layers', required=False, default=100)
    parser.add_argument('--hiddenLayerNodes', type=int, help='number of nodes in each hidden layer', required=False, default=100)

    return parser

def getJsonFiles(dataFolder):
    root = os.getcwd()
    path = os.path.join(root, dataFolder)

    jsonFiles = []
    for r, d, f in os.walk(path):
        for file in f:
            if file.endswith('.json'):
                jsonFiles.append(os.path.join(r, file))

    return jsonFiles

def extractData(jsonFiles):
    X = []
    y = []
    genreMap = {}
    # xEntry = np.zeros(1 + 1 + BEATSAMPLES*12 + BEATSAMPLES + BEATSAMPLES*6 + BEATSAMPLES + BEATSAMPLES)
    for jsonFile in jsonFiles:
        if jsonFile.endswith('mp3Metadata.json'):
            continue
        print('Extracting data for %s' % jsonFile)
        f = open(jsonFile)
        jsonData = json.load(f)
        f.close()
        xEntry = np.zeros(1 + 1 + BEATSAMPLES*12 + BEATSAMPLES + BEATSAMPLES*6 + BEATSAMPLES + BEATSAMPLES)

        beats = jsonData['beats']
        samples = beats[:BEATSAMPLES] # TODO what if BEATSAMPLES > len(beats)
        if len(samples) < BEATSAMPLES:
            # song too short, throw out
            continue
        idx = 0
        # tempo
        xEntry[idx] = jsonData['tempo']
        idx += 1
        # tuning
        xEntry[idx] = jsonData['tuning']
        idx += 1
        # chromagram samples 12 bands
        # numSamples = len(jsonData['chromagram'][0])
        for i in range(12):
            for counter in range(len(samples)):
                jsonIdx = samples[counter]
                xEntry[idx] = jsonData['chromagram'][i][jsonIdx]
                idx += 1

        # spec_bw samples 
        # numSamples = len(jsonData['spec_bw'][0])
        # maxSample = np.max(jsonData['spec_bw'])
        for counter in range(len(samples)):
            jsonIdx = samples[counter]
            xEntry[idx] = jsonData['spec_bw'][0][jsonIdx]
            idx += 1
        # constrast samples 6 bands
        # numSamples = len(jsonData['contrast'][0])
        # maxSample = np.max(jsonData['contrast'])
        for i in range(6):
            for counter in range(len(samples)):
                jsonIdx = samples[counter]
                xEntry[idx] = jsonData['contrast'][i][jsonIdx]
                idx += 1

        # maxRolloff samples
        # numSamples = len(jsonData['maxRolloff'][0])
        # maxSample = np.max(jsonData['maxRolloff'])
        for counter in range(len(samples)):
            jsonIdx = samples[counter]
            xEntry[idx] = jsonData['maxRolloff'][0][jsonIdx]
            idx += 1
        # minRolloff samples
        # numSamples = len(jsonData['minRolloff'][0])
        # maxSample = np.max(jsonData['minRolloff'])
        for counter in range(len(samples)):
            jsonIdx = samples[counter]
            xEntry[idx] = jsonData['minRolloff'][0][jsonIdx]
            idx += 1
        assert idx == len(xEntry)
        X.append(xEntry)
        if jsonData['genre'] not in genreMap:
            genreMap[jsonData['genre']] = len(genreMap)
        y.append(genreMap[jsonData['genre']])

    return X, y, genreMap

def main(args):
    dataFolder = args.data
    jsonFilenames = getJsonFiles(dataFolder)
    X, y, genreMap = extractData(jsonFilenames)

    X = normalize(X, axis=0)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    # for train_index, val_index in kf.split(X, y):
    #     X_train, X_val = X[train_index,:], X[val_index,:]
    #     y_train, y_val = y[train_index], y[val_index]

    clf = Perceptron(max_iter=1000)

    clf.fit(X_train, y_train)
    
    preds = clf.predict(X_test)

    print(clf.score(X_test, y_test))
    correct = 0
    for (label, pred) in zip(y_test, preds):
        if pred == label:
            correct += 1
    
    print((correct/len(y_test))*100)
    print(preds)
    print(y_test)

if __name__ == '__main__':
    parser = getArgParser()
    args = parser.parse_args()
    main(args)