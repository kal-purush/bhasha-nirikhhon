namespace ValidAnagram;

public static class AnagramValidationExtension
{
    // check Anagram ignoring all non-letter symbols
    // Algorithm complexity O(n)
    // Space complexity O(1) - because we need only the number of unique letters from our language
    public static bool isValidAnagramToWord(this string str1, string str2)
    {
        Dictionary<char, int> wordDict = new();
        foreach (var character in str1) // O(n)
        {
            if (!char.IsLetter(character)) continue; // O(1), works for Ascii and Unicode
            if (wordDict.ContainsKey(character)) // O(1)
            {
                wordDict[character] += 1;
                continue;
            }
            wordDict.Add(character, 1);
        }

        foreach (var character in str2) // O(n)
        {
            if (!char.IsLetter(character)) continue; // O(1), works with Ascii and Unicode
            if (!wordDict.ContainsKey(character)) // O(1)
            {
                return false;
            }

            wordDict[character] -= 1;
        }

        foreach (var charCount in wordDict)
        {
            if (charCount.Value != 0)
            {
                return false;
            }
        }

        return true;
    }
}