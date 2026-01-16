import operator

fractions = []

ofInterest = []
numerators = []
denominators = []

for i in range(10, 100):
	for j in range(i+1, 100):
		pair = (i, j)
		fractions.append(pair)

for numerator, denominator in fractions:
  try:
    value = float(numerator)/denominator
    numeratorSet = set(list(str(numerator)))
    denominatorSet = set(list(str(denominator)))
    sharedDigitSet = numeratorSet.intersection(denominatorSet)
    if len(sharedDigitSet) == 0:
      continue
    if len(sharedDigitSet) == 2:
      continue
    
    sharedDigit = list(sharedDigitSet)[0]
    
    if sharedDigit == "0":
      continue
    
    numeratorWithoutDigit = list(str(numerator))
    numeratorWithoutDigit.remove(sharedDigit)
    denominatorWithoutDigit = list(str(denominator))
    denominatorWithoutDigit.remove(sharedDigit)
    
    newValue = float(int(numeratorWithoutDigit[0]))/int(denominatorWithoutDigit[0])
    
    if value == newValue:
      ofInterest.append( (numerator, denominator) )
      numerators.append(numerator)
      denominators.append(denominator)
  except ZeroDivisionError:
    continue
  
totalNumerator = reduce(operator.mul, numerators, 1)
totalDenominator = reduce(operator.mul, denominators, 1)

print float(totalDenominator)/totalNumerator