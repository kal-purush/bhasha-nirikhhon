__author__ = 'liushuman'

import theano
import theano.tensor as T

import sys
sys.path.append("..")
from models.DNNUnit import DNNUnit
from models.Embedding import Embedding
from models.GRUUnit import Gate, GRUUnit
from models.Pooling import Pooling


class TestModels(object):

    def __init__(self):
        self.params = []

    def test_dnn(self):
        dnn_input = T.matrix()

        dnn_unit = DNNUnit((3, 3), (2, 3), if_bias=True, activate_type='tanh')
        dnn_output = dnn_unit.apply(dnn_input)

        fn = theano.function([dnn_input], dnn_output)
        print fn([[1, 1, 1], [2, 2, 2], [3, 3, 3]])

    def test_emb(self):
        indexs = T.imatrix()

        emb = Embedding((5, 10))
        indexs_emb = emb.apply(indexs)

        fn = theano.function([indexs], indexs_emb)
        print fn([[1, 2, 3]])
        print fn([[1], [2], [3]])

    def test_gate(self):
        xt = T.matrix()
        htm1 = T.matrix()
        context = T.matrix()

        gate_without_context = Gate((4, 2), (2, 2), if_bias=True, context=None, activate_type='sigmoid')
        gate_with_context = Gate((4, 2), (2, 2), if_bias=True, context=context, activate_type='tanh')

        output_without_context = gate_without_context.apply(xt, htm1)
        output_with_context = gate_with_context.apply(xt, htm1)

        fn = theano.function([xt, htm1, context], [output_without_context, output_with_context])

        print fn([[1, 1], [2, 2], [3, 3], [4, 4]], [[0, 0], [0, 0]], [[1, 1], [1, 1]])

    def test_gru(self):
        sequences = T.tensor3()
        context = T.matrix()

        gru_without_context = GRUUnit((4, 1), (2, 1), if_bias=True, context=None, activate_type='tanh')
        output_without_context = gru_without_context.apply(sequences)

        gru_with_context = GRUUnit((4, 1), (2, 1), if_bias=True, context=output_without_context[-1], activate_type='tanh')
        output_with_context = gru_with_context.merge_out(sequences, (3, 8), (3, 1))

        fn = theano.function([sequences], [output_without_context, output_with_context])

        print fn([[[1], [1], [1], [1]],
                  [[2], [2], [2], [2]],
                  [[3], [3], [3], [3]]],)
                 # [[10], [10]])

    def test_pooling(self):
        func_input = T.matrix()

        max_output = Pooling().apply(func_input, pooling_type='max', axis=0)
        mean_output = Pooling().apply(func_input, pooling_type='mean', axis=1)

        fn = theano.function([func_input], [max_output, mean_output])
        print fn([[2, 2, 3], [1, 5, 6]])


if __name__ == "__main__":
    test_models = TestModels()

    # test_models.test_dnn()
    # test_models.test_emb()
    # test_models.test_gate()
    test_models.test_gru()
    # test_models.test_pooling()