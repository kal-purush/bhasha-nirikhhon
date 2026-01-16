using System;

/// <summary>
/// Dungeons and Dragons 5th Edition Dice Namespace.
/// </summary>
namespace DnD5.Dice
{
    /// <summary>
    /// Static random number generating assist object.
    /// </summary>
    public static class Dice
    {
        /// <summary>
        /// Random seeded engine internal to a Dice object.
        /// </summary>
        private static readonly Random Rng = new Random();

        /// <summary>
        /// Returns to the caller a randomly generated number ranging from 1-2.
        /// </summary>
        public static int D2 => Rng.Next(1, 3);

        /// <summary>
        /// Return to the caller a randomly generated number ranging from 1-3.
        /// </summary>
        public static int D3 => Rng.Next(1, 4);

        /// <summary>
        /// Returns to the caller a randomly generated number ranging from 1-4.
        /// </summary>
        public static int D4 => Rng.Next(1, 5);

        /// <summary>
        /// Returns to the caller a randomly generated number ranging from 1-6.
        /// </summary>
        public static int D6 => Rng.Next(1, 7);

        /// <summary>
        /// Returns to the caller a randomly generated number ranging from 1-8.
        /// </summary>
        public static int D8 => Rng.Next(1, 9);

        /// <summary>
        /// Returns to the caller a randomly generated number ranging from 1-10.
        /// </summary>
        public static int D10 => Rng.Next(1, 11);

        /// <summary>
        /// Returns to the caller a randomly generated number ranging from 1-12.
        /// </summary>
        public static int D12 => Rng.Next(1, 13);

        /// <summary>
        /// Returns to the caller a randomly generated number ranging from 1-20.
        /// </summary>
        public static int D20 => Rng.Next(1, 21);

        /// <summary>
        /// Returns to the caller a randomly generated number ranging from 1-100.
        /// </summary>
        public static int D100 => Rng.Next(1, 101);
    }
}