namespace ValidPalindrome;

internal class PalindromeValidation
{
    // Algorithm Complexity: O(n)
    // Space Complexity: O(n)
    public static bool ValidateUsingCharArray(string inputString)
    {
        if (inputString.Length <= 1)
        {
            return false;
        }

        
        var inputArray = inputString.Where(char.IsLetter).ToArray();
        Console.WriteLine($"array: {new string(inputArray)}");
        var leftPointer = 0;
        var rightPointer = inputArray.Length - 1;

        while (leftPointer < rightPointer)
        {
            if (char.ToUpperInvariant(inputArray[leftPointer]) != char.ToUpperInvariant(inputArray[rightPointer]))
            {
                return false;
            }

            leftPointer++;
            rightPointer--;
        }
        
        return true;
    }

    // Algorithm Complexity: O(n)
    // Space Complexity: o(1)
    public static bool ValidateIgnoringNonCharSymbols(string inputString)
    {
        if (inputString.Length <= 1)
        {
            return false;
        }

        var leftPointer = 0;
        var rightPointer = inputString.Length - 1;

        while (leftPointer < rightPointer)
        {
            while (leftPointer < rightPointer)
            {
                if (char.IsLetter(inputString[leftPointer]))
                {
                    break;
                }

                leftPointer++;
            }
            
            while (leftPointer < rightPointer)
            {
                if (char.IsLetter(inputString[rightPointer]))
                {
                    break;
                }

                rightPointer--;
            }
            
            if (char.ToUpperInvariant(inputString[leftPointer]) != char.ToUpperInvariant(inputString[rightPointer]))
            {
                return false;
            }

            leftPointer++;
            rightPointer--;
        }

        return true;
    }
}