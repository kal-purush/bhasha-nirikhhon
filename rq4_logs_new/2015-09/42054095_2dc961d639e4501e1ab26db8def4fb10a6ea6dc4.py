from __future__ import division
import codecs
import numpy as np
import sys
import matplotlib.pyplot as plt

class StructuredPerceptron:

    def __init__(self, epochs=10, eta=1.):

        self.parameters_for_epoch = []

        self.n_epochs = epochs
        self.eta = eta

    def train(self, data, heldout, verbose=False, run_label=None):

        self.w = np.zeros(self.n_features, dtype=float)

        training_accuracy = [0.0]
        heldout_accuracy  = [0.0]

        for i_epoch in xrange(self.n_epochs):

            incorrect = 0.
            total     = 0.

            for compound in data:
                total, incorrect = self.train_one(compound, total, incorrect)

            self.parameters_for_epoch.append(self.w.copy())

            accuracy = 1.0 - (incorrect/total)
            training_accuracy.append(accuracy)

            if verbose:
                _, _, acc = self.test(heldout)
                heldout_accuracy.append(acc)

            #Stop if the error on the training data does not decrease
            if training_accuracy[-1] <= training_accuracy[-2]:
                break

            print >>sys.stderr, "Epoch %i, Accuracy: %f" % (i_epoch, accuracy)

        #Average!
        averaged_parameters = 0
        for epoch_parameters in self.parameters_for_epoch:
            averaged_parameters += epoch_parameters
        averaged_parameters /= len(self.parameters_for_epoch)

        self.w = averaged_parameters

        #Finished training
        self.trained = True

        #Export training info in verbose mode:
        if verbose:
            x = np.arange(0, len(training_accuracy), 1.0)
            plt.plot(x, training_accuracy, marker='o', linestyle='--', color='r', label='Training')
            plt.plot(x, heldout_accuracy,  marker='o', linestyle='--', color='b', label='Heldout')

            plt.xlabel('Epoch')
            plt.ylabel('Accuracy')
            plt.title('Training and Heldout Accuracy')

            plt.ylim([0.9, 1.0])

            plt.legend(bbox_to_anchor=(1., 0.2))

            plt.savefig('eval/%s_training.png' % run_label)

            plt.close()

    def viterbi_decode(compound):



    def train_one(self, compound, total, incorrect):

        #Returns a list of tuples with (start, stop) position
        z = self.viterbi_decode(compound)
        y = compound.gold_lattice

        for (i, z_i) in enumerate(z):

            y_i = compound.gold_lattice.closest_gold_split(z_i)

            total += 1.

            if i == 0:
                y_prev = (0,0)
                z_prev = (0,0)
            else:
                y_prev = 
                z_prev = z[i-1]

            if compound.gold_lattice.contains(z_i):

                #The predicted split was not correct
                incorrect += 1.

                correct_split_features   = fs(compound, y_prev, y_i)
                self.w[correct_split_features]   += self.eta

                predicted_split_features = fs(compound, z_prev, z_i)
                self.w[predicted_split_features] -= self.eta

        return total, incorrect



if __name__ == '__main__':

    lattices = map(Lattice, codecs.open("data/").readlines())

    trainer = StructuredPerceptron(epochs=10)
    trainer.train(train, heldout, verbose=True)