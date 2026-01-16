import re
import matplotlib.pyplot as plt
import math


def readLogFile(dataset):
    filename = f'./Datasets/Binary/{dataset}/log.txt'
    content = ""
    with open(filename, "r") as f:
        content = f.read()
    return content


def createLossPlot(values, dataset, y_lim_values):
    plt.figure()
    plt.plot(values[0], label='Zbiór treningowy')
    plt.plot(values[1], label='Zbiór walidacyjny')
    plt.set_ylim(y_lim_values[0], y_lim_values[1])
    plt.title("Funkcja straty")
    plt.xlabel('Liczba epok')
    plt.ylabel('Wartość')
    plt.legend(frameon=False)
    plt.savefig(f'./Datasets/Binary/{dataset}/loss.pdf',
                format="pdf", bbox_inches="tight")


def createAccPlot(values, dataset, y_lim_values):
    plt.figure()
    plt.plot(values[0], label='Zbiór treningowy')
    plt.plot(values[1], label='Zbiór walidacyjny')
    plt.set_ylim(y_lim_values[0], y_lim_values[1])
    plt.title("Dokładność")
    plt.xlabel('Liczba epok')
    plt.ylabel('Dokładność [%]')
    plt.legend(frameon=False)
    plt.savefig(f'./Datasets/Binary/{dataset}/acc.pdf',
                format="pdf", bbox_inches="tight")


datasets = ['OnlySumsDiff-640x480-10sec']


for dataset in datasets:
    logFiles = []
    for wavelet in ['Ricker', 'Morlet']:
        logFiles.append(readLogFile(f'{dataset}/{wavelet}'))
    accuracy = []
    loss = []
    for logFile in logFiles:
        res = re.search(r'\[[0-9]*.', logFile)
        start = res.start()
        end = logFile.find('Training complete in')
        arrays = logFile[start:end].split('\n')
        def convertToFloat(l): return [float(x) for x in l]

        def createListFromString(x): return convertToFloat(
            x.strip('][').split(', '))

        loss.append([createListFromString(arrays[0]),
                     createListFromString(arrays[2])])

        def trimTensor(x): return x.replace(
            'tensor(', '').replace(', dtype=torch.float64)', '')
        arrays[1] = trimTensor(arrays[1])
        arrays[3] = trimTensor(arrays[3])
        accuracy.append([createListFromString(arrays[1]),
                         createListFromString(arrays[3])])

    rickerAcc = accuracy[0]
    morletAcc = accuracy[1]
    rickerLoss = loss[0]
    morletLoss = loss[1]

    def flatList(x): return [item for sublist in x for item in sublist]

    allAcc = flatList([*rickerAcc, *morletAcc])
    allLoss = flatList([*rickerLoss, *morletLoss])
    allAccMin = math.ceil(min(allAcc))
    allAccMax = round(max(allAcc))
    allLossMin = math.ceil(min(allLoss)*100)/100
    allLossMax = round(max(allLoss), 2)

    fig, axs = plt.subplots(2, 2)
    rickerAccFig = axs[0, 0]
    rickerAccFig.set_title('Falka Rickera')
    rickerAccFig.plot(rickerAcc[0], label='Zbiór treningowy')
    rickerAccFig.plot(rickerAcc[1], label='Zbiór walidacyjny')
    rickerAccFig.set_ylim(allAccMin, allAccMax)
    morletAccFig = axs[0, 1]
    morletAccFig.set_title('Falka Morleta')
    morletAccFig.plot(morletAcc[0], label='Zbiór treningowy')
    morletAccFig.plot(morletAcc[1], label='Zbiór walidacyjny')
    morletAccFig.set_ylim(allAccMin, allAccMax)
    rickerLossFig = axs[1, 0]
    rickerLossFig.plot(rickerLoss[0], label='Zbiór treningowy')
    rickerLossFig.plot(rickerLoss[1], label='Zbiór walidacyjny')
    rickerLossFig.set_ylim(allLossMin, allLossMax)
    morletLossFig = axs[1, 1]
    morletLossFig.plot(morletLoss[0], label='Zbiór treningowy')
    morletLossFig.plot(morletLoss[1], label='Zbiór walidacyjny')
    morletLossFig.set_ylim(allLossMin, allLossMax)
    fig.text(0.5, 0.04, 'Liczba epok', ha='center')
    fig.text(0.02, 0.3, 'Funkcja straty', va='center', rotation='vertical')
    fig.text(0.02, 0.7, 'Dokładność (%)', va='center', rotation='vertical')
    plt.legend(frameon=False)
    plt.show()