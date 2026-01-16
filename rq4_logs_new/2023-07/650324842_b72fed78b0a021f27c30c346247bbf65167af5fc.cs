namespace NumberOfGoodPairs;

internal static class NumberOfGoodPairs
{
    
    // Algorithm Complexity O(n)
    // Space Complexity O(1)
    public static int CountTheNumberOfGoodPairsNaive(int[] arrayOfNumbers)
    {
        var numberOfGoodPairs = 0;
        Dictionary<int, int> uniqueDigitsCount = new();
        for(int i = 0; i < arrayOfNumbers.Length; i++) // Alg: O(n)
        {
            if (uniqueDigitsCount.ContainsKey(arrayOfNumbers[i]))
            {
                uniqueDigitsCount[arrayOfNumbers[i]] += 1;
                continue;
            }
            
            uniqueDigitsCount.Add(arrayOfNumbers[i], 1);
        }
        
        foreach (var keyValuePair in uniqueDigitsCount) // Alg: O(1), Worst scenario O(n)
        {
            for (var i = keyValuePair.Value - 1; i >= 1; i--)
            {
                numberOfGoodPairs += i;
            }
        }

        return numberOfGoodPairs;
    }
    
    // Algorithm Complexity O(n)
    // Space Complexity O(1)
    public static int CountTheNumberOfGoodPairsOptimized(int[] arrayOfNumbers)
    {
        var numberOfGoodPairs = 0;
        Dictionary<int, int> uniqueDigitsCount = new();
        for(int i = 0; i < arrayOfNumbers.Length; i++) // Alg: O(n)
        {
            if (uniqueDigitsCount.ContainsKey(arrayOfNumbers[i]))
            {
                uniqueDigitsCount[arrayOfNumbers[i]] += 1;

                numberOfGoodPairs += uniqueDigitsCount[arrayOfNumbers[i]] - 1;
                continue;
            }
            
            uniqueDigitsCount.Add(arrayOfNumbers[i], 1);
        }

        return numberOfGoodPairs;
    }
}