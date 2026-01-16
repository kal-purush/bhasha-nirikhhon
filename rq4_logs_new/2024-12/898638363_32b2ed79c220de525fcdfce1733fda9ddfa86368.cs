using System;
using System.Collections.Generic;
using Soenneker.Extensions.String;

namespace Soenneker.Utils.Strings.DiceCoefficient;

/// <summary>
/// A utility library for comparing strings via the Dice Coefficient algorithm.
/// </summary>
public static class DiceCoefficientStringUtil
{
    /// <summary>
    /// Calculates the similarity percentage between two strings via Dice Coefficient.
    /// </summary>
    /// <param name="s1">The first string.</param>
    /// <param name="s2">The second string.</param>
    /// <returns>The similarity percentage between the two strings.</returns>
    public static double CalculatePercentage(string s1, string s2)
    {
        double similarity = Calculate(s1, s2);
        return similarity * 100;
    }

    public static double Calculate(string s1, string s2)
    {
        bool isS1Empty = s1.IsNullOrEmpty();
        bool isS2Empty = s2.IsNullOrEmpty();

        if (isS1Empty || isS2Empty)
            return isS1Empty && isS2Empty ? 1.0 : 0.0;

        // Generate bigrams and calculate intersection simultaneously
        Dictionary<string, int> bigrams1 = GetBigramsWithFrequency(s1, out int totalBigrams1);
        Dictionary<string, int> bigrams2 = GetBigramsWithFrequency(s2, out int totalBigrams2);
        int intersectionSize = CountIntersection(bigrams1, bigrams2);

        // Calculate the Dice Coefficient
        double diceCoefficient = (2.0 * intersectionSize) / (totalBigrams1 + totalBigrams2);

        return diceCoefficient;
    }

    private static Dictionary<string, int> GetBigramsWithFrequency(string input, out int totalFrequency)
    {
        var bigrams = new Dictionary<string, int>();
        totalFrequency = 0;

        for (var i = 0; i < input.Length - 1; i++)
        {
            var bigram = input.AsSpan(i, 2).ToString();

            if (!bigrams.TryAdd(bigram, 1))
            {
                bigrams[bigram]++;
            }

            totalFrequency++;
        }

        return bigrams;
    }

    private static int CountIntersection(Dictionary<string, int> bigrams1, Dictionary<string, int> bigrams2)
    {
        var intersectionCount = 0;

        foreach (KeyValuePair<string, int> kvp in bigrams1)
        {
            if (bigrams2.TryGetValue(kvp.Key, out int frequencyInBigrams2))
            {
                intersectionCount += Math.Min(kvp.Value, frequencyInBigrams2);
            }
        }

        return intersectionCount;
    }
}