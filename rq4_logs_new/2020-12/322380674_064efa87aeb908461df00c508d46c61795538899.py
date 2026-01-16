import random, unittest, math
from errpp import *

def value_with_error(min = -100, max = 100, factor = 1):
    value = random.uniform(min,max)
    error = random.uniform(0,abs(value)*factor)
    return value, error

class ErrorPropagation(unittest.TestCase):
    __TRIALS = 100000

    def round_dec(self, n, decs = 5):
        return round(n, decs)

    def compare_values_and_errors(self, x, y):
        self.assertEqual(self.round_dec(x.value), self.round_dec(y.value),
                         "[value] expected = {0} ,, calculated = {1}".format(self.round_dec(x.value), self.round_dec(y.value)))
        self.assertEqual(self.round_dec(x.error), self.round_dec(y.error),
                         "[error] expected = {0} ,, calculated = {1}".format(self.round_dec(x.value), self.round_dec(y.value)))

    def test_add_error_strict_estimation_order(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            k = StatisticalError(*a) + StatisticalError(*b)
            l = WorstCaseError(*a) + WorstCaseError(*b)
            m = ExtremeError(*a) + ExtremeError(*b)

            self.assertEqual(k.value, l.value)
            self.assertEqual(l.value, m.value)

            self.assertLessEqual(abs(k.error), abs(l.error))
            self.assertLessEqual(abs(l.error), abs(m.error))

    def test_sub_error_strict_estimation_order(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            k = StatisticalError(*a) - StatisticalError(*b)
            l = WorstCaseError(*a) - WorstCaseError(*b)
            m = ExtremeError(*a) - ExtremeError(*b)

            self.assertEqual(k.value, l.value)
            self.assertEqual(l.value, m.value)

            self.assertLessEqual(abs(k.error), abs(l.error))
            self.assertLessEqual(abs(l.error), abs(m.error))

    def test_mul_error_strict_estimation_order(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            k = StatisticalError(*a) * StatisticalError(*b)
            l = WorstCaseError(*a) * WorstCaseError(*b)
            m = ExtremeError(*a) * ExtremeError(*b)

            self.assertEqual(k.value, l.value)
            self.assertEqual(l.value, m.value)

            self.assertLessEqual(k.error, l.error)
            self.assertLessEqual(l.error, m.error)

    def test_div_error_strict_estimation_order(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            k = StatisticalError(*a) / StatisticalError(*b)
            l = WorstCaseError(*a) / WorstCaseError(*b)
            m = ExtremeError(*a) / ExtremeError(*b)

            self.assertEqual(k.value, l.value)
            self.assertEqual(l.value, m.value)

            self.assertLessEqual(k.error, l.error, "is")
            self.assertLessEqual(l.error, m.error, "as")

    def test_StatisticalError_add_error_correctness(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            expected = WorstCaseError(a[0] + b[0], math.sqrt(a[1]**2 + b[1]**2))
            calculated = StatisticalError(*a) + StatisticalError(*b)

            self.compare_values_and_errors(expected, calculated)

    def test_StatisticalError_sub_error_correctness(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            expected = WorstCaseError(a[0] - b[0], math.sqrt(a[1]**2 + b[1]**2))
            calculated = StatisticalError(*a) - StatisticalError(*b)

            self.compare_values_and_errors(expected, calculated)

    def test_StatisticalError_mul_error_correctness(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            result = a[0] * b[0]
            expected = WorstCaseError(result, abs(result)*math.sqrt((a[1]/a[0])**2 + (b[1]/b[0])**2))
            calculated = StatisticalError(*a) * StatisticalError(*b)

            self.compare_values_and_errors(expected, calculated)

    def test_StatisticalError_div_error_correctness(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            result = a[0] / b[0]
            expected = WorstCaseError(result, abs(result)*math.sqrt((a[1]/a[0])**2 + (b[1]/b[0])**2))
            calculated = StatisticalError(*a) / StatisticalError(*b)

            self.compare_values_and_errors(expected, calculated)

    def test_StatisticalError_neg_value_correctness(self):
        for _ in range(self.__TRIALS):
            gen = value_with_error()

            expected = StatisticalError(-gen[0],gen[1])
            calculated = -StatisticalError(*gen)

            self.compare_values_and_errors(expected, calculated)

    def test_WorstCaseError_add_error_correctness(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            expected = WorstCaseError(*map(sum, zip(a, b)))
            calculated = WorstCaseError(*a) + WorstCaseError(*b)

            self.compare_values_and_errors(expected, calculated)

    def test_WorstCaseError_sub_error_correctness(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            expected = WorstCaseError(a[0] - b[0], a[1] + b[1])
            calculated = WorstCaseError(*a) - WorstCaseError(*b)

            self.compare_values_and_errors(expected, calculated)

    def test_WorstCaseError_mul_error_correctness(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            result = a[0] * b[0]
            expected = WorstCaseError(result, abs(result)*(a[1]/abs(a[0]) + b[1]/abs(b[0])))
            calculated = WorstCaseError(*a) * WorstCaseError(*b)

            self.compare_values_and_errors(expected, calculated)

    def test_WorstCaseError_div_error_correctness(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            result = a[0] / b[0]
            expected = WorstCaseError(result, abs(result)*(a[1]/abs(a[0]) + b[1]/abs(b[0])))
            calculated = WorstCaseError(*a) / WorstCaseError(*b)

            self.compare_values_and_errors(expected, calculated)

    def test_WorstCaseError_neg_value_correctness(self):
        for _ in range(self.__TRIALS):
            gen = value_with_error()

            expected = WorstCaseError(-gen[0],gen[1])
            calculated = -WorstCaseError(*gen)

            self.compare_values_and_errors(expected, calculated)

    def test_ExtremeError_add_error_correctness(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            expected = ExtremeError(*map(sum, zip(a,b)))
            calculated = ExtremeError(*a) + ExtremeError(*b)

            self.compare_values_and_errors(expected, calculated)


    def test_ExtremeError_sub_error_correctness(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            expected = ExtremeError(a[0] - b[0], a[1] + b[1])
            calculated = ExtremeError(*a) - ExtremeError(*b)

            self.compare_values_and_errors(expected, calculated)

    def test_ExtremeError_mul_error_correctness(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            result = a[0] * b[0]
            expected = ExtremeError(result, abs(result)*(a[1]/abs(a[0]) + b[1]/abs(b[0]) + a[1]*b[1]/abs(a[0]*b[0])))
            calculated = ExtremeError(*a) * ExtremeError(*b)

            self.compare_values_and_errors(expected, calculated)

    def test_ExtremeError_div_error_correctness(self):
        for _ in range(self.__TRIALS):
            a, b = value_with_error(), value_with_error()

            result = a[0] / b[0]
            lim1 = (abs(a[0]) + a[1]) / (abs(b[0]) - b[1]) - abs(result)
            lim2 = (abs(a[0]) - a[1]) / (abs(b[0]) + b[1]) - abs(result)
            expected = ExtremeError(result, max(lim1, lim2))
            calculated = ExtremeError(*a) / ExtremeError(*b)

            self.compare_values_and_errors(expected, calculated)

    def test_ExtremeError_neg_value_correctness(self):
        for _ in range(self.__TRIALS):
            gen = value_with_error()

            expected = ExtremeError(-gen[0],gen[1])
            calculated = -ExtremeError(*gen)

            self.compare_values_and_errors(expected, calculated)
