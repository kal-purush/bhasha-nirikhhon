using System;

namespace SimpleNeuralNetwork
{
    static class RNG
    {
        private static readonly Random _rng = new Random();

        public static double NextDouble()
        {
            var rand = _rng.NextDouble() * 2 - 1;
            if (rand == 1)
            {
                return rand - double.Epsilon;
            }

            if (rand == -1)
            {
                return rand + double.Epsilon;
            }
            return rand;
        }
    }
}