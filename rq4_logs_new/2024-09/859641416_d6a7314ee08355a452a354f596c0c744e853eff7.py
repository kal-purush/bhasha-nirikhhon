from abc import abstractmethod, ABC
import numpy as np

class Aggregator:
    def __init__(self, name):
        """
        Base aggregator class.
        :param name: The name of the aggregator.
        """
        self.name = name

    @abstractmethod
    def evaluate(self, values, weigths):
        pass

class UGCDAggregator(Aggregator):
    def __init__(self, name, fixed_andness):
        """
        Base class for individual UGCD aggregators from C to D.
        :param name: The name of the aggregator (e.g., "C", "D").
        :param fixed_andness: The fixed andness level for this aggregator.
        """
        super().__init__(name)
        self.fixed_andness = fixed_andness

    @staticmethod
    def ugcd(alpha, values, weights):
        """
        UGCD function modified to handle multiple inputs and corresponding weights.

        :param alpha: The andness level (alpha), should be between 0 and 1.
        :param values: A list of input values.
        :param weights: A list of weights associated with the input values.
                        Must sum to 1.
        :return: The aggregated value based on the UGCD formula.
        """
        values = np.array(values)
        weights = np.array(weights)
        if not np.isclose(np.sum(weights), 1.0):
            print(weights)
        assert np.isclose(np.sum(weights), 1.0), "Weights must sum to 1."

        def r2(alpha):
            """
            This function handles the nested expression based on the value of alpha for the exponent.
            """
            beta = 0.5 - alpha
            return (0.25 + beta * (1.65811 + beta * (2.15388 + beta * (8.2844 + 6.16764 * beta)))) / (
                        alpha * (1 - alpha))

        R = 0.7201  # Fixed value for

        if alpha == 1:
            return np.min(values)  # Full conjunction case

        if 0.75 <= alpha < 1:
            # Formula for 1/2 <= α <= 3/4 power mean aggregator
            weighted_values = np.sum(weights * (values ** r2(alpha)))
            return weighted_values ** (1 / r2(alpha))

        if 0.5 <= alpha <= 0.75:
            # Formula for 3/4 < α < 1 linear interpolation between arithmetic mean and power mean
            part1 = (3 - 4 * alpha) * np.sum(weights * values)
            part2 = (4 * alpha - 2) * np.sum(weights * (values ** R)) ** (1 / R)
            return part1 + part2

        if 0 <= alpha < 0.5:
            return 1 - UGCDAggregator.ugcd(1 - alpha, 1 - values, weights)

        raise ValueError("Unsupported value of alpha.")

    def evaluate(self, values, weights):
        """
        Evaluates the aggregation based on the fixed andness, weights, and values.
        :param weights: The weights for the inputs.
        :param values: The values to aggregate.
        :return: Aggregated result.
        """
        return UGCDAggregator.ugcd(self.fixed_andness, values, weights)


# 15 Aggregator Classes with fixed andness levels and andness step of 1/14
class FullConjunction(UGCDAggregator):
    def __init__(self):
        super().__init__("C", 1)


class HighHardPartialConjunction(UGCDAggregator):
    def __init__(self):
        super().__init__("HC+", 13 / 14)


class MediumHardPartialConjunction(UGCDAggregator):
    def __init__(self):
        super().__init__("HC", 12 / 14)


class LowHardPartialConjunction(UGCDAggregator):
    def __init__(self):
        super().__init__("HC-", 11 / 14)


class HighSoftPartialConjunction(UGCDAggregator):
    def __init__(self):
        super().__init__("SC+", 10 / 14)


class MediumSoftPartialConjunction(UGCDAggregator):
    def __init__(self):
        super().__init__("SC", 9 / 14)

class LowSoftPartialConjunction(UGCDAggregator):
    def __init__(self):
        super().__init__("SC-", 8 / 14)

class Neutrality(UGCDAggregator):
    def __init__(self):
        super().__init__("A", 0.5)

class LowSoftPartialDisjunction(UGCDAggregator):
    def __init__(self):
        super().__init__("SD-", 6 / 14)


class MediumSoftPartialDisjunction(UGCDAggregator):
    def __init__(self):
        super().__init__("SD", 5 / 14)


class HighSoftPartialDisjunction(UGCDAggregator):
    def __init__(self):
        super().__init__("SD+", 4 / 14)


class LowHardPartialDisjunction(UGCDAggregator):
    def __init__(self):
        super().__init__("HD-", 3 / 14)


class MediumHardPartialDisjunction(UGCDAggregator):
    def __init__(self):
        super().__init__("HD", 2 / 14)


class HighHardPartialDisjunction(UGCDAggregator):
    def __init__(self):
        super().__init__("HD+", 1 / 14)


class FullDisjunction(UGCDAggregator):
    def __init__(self):
        super().__init__("D", 0)


class PartialAbsorption(Aggregator, ABC):
    def __init__(self, name):
        super().__init__(name)

class ConjunctivePartialAbsorption(PartialAbsorption):
    def __init__(self, max_reward, max_penalty):
        """
        Initialize the Conjunctive Partial Absorption (CPA/AH) function.
        :param R: Maximum reward
        :param P: Maximum penalty
        """
        assert max_penalty > max_reward
        super().__init__("CPA")
        self.R = max_reward
        self.P = max_penalty
        r = self.R/0.5
        p = self.P/0.5
        self.W1 = 2 * r * (1 - p) / (p + r)
        self.W2 = (p - r) / (p - r + 2 * p * r)

    def evaluate(self, values, weights):
        x = values[0]
        y = values[1]
        part1 = (1 - self.W2) / (self.W1 * x + (1 - self.W1) * y)
        part2 = self.W2 / x
        return 1 / (part1 + part2)

class DisjunctivePartialAbsorption(PartialAbsorption):
    def __init__(self, max_reward, max_penalty):
        """
        Initialize the Disjunctive Partial Absorption (DPA/AH_) function.
        :param R: Maximum reward
        :param P: Maximum penalty
        """
        assert max_penalty < max_reward
        super().__init__("DPA")
        self.R = max_reward
        self.P = max_penalty
        r = self.R / 0.5
        p = self.P / 0.5
        self.W1 = 2 * p * (1 - r) / (r + p)
        self.W2 = (r - p) / (r - p + 2 * r * p)

    def evaluate(self, values, weights):
        x = values[0]
        y = values[1]
        part1 = (1 - self.W2) / (1 - self.W1 * x - (1 - self.W1) * y)
        part2 = self.W2 / (1 - x)
        return 1 - 1/(part1+part2)


# Example Usage
values = [0.8, 0.6, 0.9, 0.7]
weights = [0.25, 0.25, 0.25, 0.25]

# Full conjunction
full_conjunction = FullConjunction()
print("Full Conjunction Result:", full_conjunction.evaluate(values, weights))

# Hard partial conjunction (1st)
hard_partial_conjunction1 = HighHardPartialConjunction()
print("Hard Partial Conjunction 1 Result:", hard_partial_conjunction1.evaluate(values, weights))

# Soft partial disjunction (3rd)
soft_partial_disjunction3 = HighSoftPartialDisjunction()
print("Soft Partial Disjunction 3 Result:", soft_partial_disjunction3.evaluate(values, weights))

# Full disjunction
full_disjunction = FullDisjunction()
print("Full Disjunction Result:", full_disjunction.evaluate(values, weights))