#QUESTION 1
def commonElements(list1, list2):
    set1 = set(list1)
    set2 = set(list2)

    commonSet = set1.intersection(set2)
    return list(commonSet)

list1 = [1,2,3,4,5]
list2 = [1,3,4,5,7,8]

commonList = commonElements(list1,list2)
print(commonList)

#QUESTION 2
def palindromesFinder(Listt):

    palindromeList = []

    for string in Listt:
        if string == string[::-1]:
            palindromeList.append(string)

    return palindromeList

myList = ['mom','dad','car','pycharm','coding']

print(palindromesFinder(myList))

#QUESTION3
def sieveOfEratosthenes(numbers):
    is_prime = [True] * (max(numbers) + 1)
    is_prime[0] = False
    is_prime[1] = False

    for i in range(2, int(max(numbers) ** 0.5) + 1):
        if is_prime[i]:
            for j in range(i ** 2, max(numbers) + 1, i):
                is_prime[j] = False

    primes = []
    for number in numbers:
        if is_prime[number]:
            primes.append(number)

    return primes

numbers = [1,2,3,5,7,8,12,13,16,17,19,22,25,27,31]

primes = sieveOfEratosthenes(numbers)
print(primes)

#QUESTION4
def anagramsFinder(word, wordList):
    sorted_word = sorted(word.lower().replace(" ", ""))

    anagramsList = []

    for w in wordList:
        sorted_w = sorted(w.lower().replace(" ", ""))

        if sorted_w == sorted_word:
            anagramsList.append(w)

    return anagramsList

word = 'listen'
wordList = ['enlist','inlets','car','misery']
print(anagramsFinder(word, wordList))