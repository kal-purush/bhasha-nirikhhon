using System.Collections;
using System.Collections.Generic;
using System;

/// <summary>
/// Static class of common functions to be used throughout the game
/// </summary>
public static class CommonFunctions {

    /// <summary>
    /// Shuffle function using Fisher-Yates algorithm from SO
    /// </summary>
    /// <typeparam name="T">Any type</typeparam>
    /// <param name="rng">new Random()</param>
    /// <param name="array">Array to be shuffled</param>
    public static void Shuffle<T>(this Random rng, T[] array) {
        int n = array.Length;
        while (n > 1) {
            int k = rng.Next(n--);
            T temp = array[n];
            array[n] = array[k];
            array[k] = temp;
        }
    }
}