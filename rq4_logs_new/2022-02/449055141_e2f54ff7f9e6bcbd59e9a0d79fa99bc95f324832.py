from collections import Counter


class Estatistica:
    @classmethod
    def media(cls, numbers):
        return sum(numbers) / len(numbers)

    @classmethod
    def mediana(cls, numbers):
        numbers.sort()
        index = len(numbers) // 2
        if len(numbers) % 2 == 0:
            return (numbers[index - 1] + numbers[index]) / 2

        return numbers[index]

    @classmethod
    def moda(cls, numbers):
        number, _ = Counter(numbers).most_common()[0]
        return number


print(Estatistica.media([2, 2, 2, 4, 2, 4, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3]))


print(Estatistica.mediana([2, 2, 2, 4, 2, 4, 2, 5, 3, 3, 3, 3, 3, 3, 3]))


print(Estatistica.moda([2, 2, 2, 4, 2, 4, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3]))