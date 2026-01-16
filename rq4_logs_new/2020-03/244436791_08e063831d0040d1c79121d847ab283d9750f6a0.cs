using System;
using System.Collections.Generic;

namespace RandomGen
{
    public class Randos
    {
        // Geenerate Int
        public static int Generate(int rangeMin, int rangeMax)
        {
            var _random = new Random();
            int _randomNum = _random.Next(rangeMin, rangeMax);
            return _randomNum;
        }

        // generate decimal
        public static decimal Generate(decimal rangeMin, decimal rangeMax)
        {
            var _random = new Random();
            decimal _randomNum = ((decimal)_random.NextDouble() * (rangeMax - rangeMin) + rangeMin);
            _randomNum = (decimal)Math.Round(_randomNum, 2);

            return _randomNum;
        }

        // generate random numbers within a range with a seed (int)
        public static int Generate(int seed, int rangeMin, int rangeMax)
        {
            var _random = new Random(seed);
            int _randomNum = _random.Next(rangeMin, rangeMax);
            return _randomNum;
        }

        // generate random numbers within a range with a seed (decimal)
        public static decimal Generate(decimal seed, double rangeMin, double rangeMax)
        {
            var _random = new Random((int)seed);

            decimal _randomNum = (decimal)(_random.NextDouble() * (rangeMax - rangeMin) + rangeMin);
            _randomNum = (decimal)Math.Round(_randomNum, 2);

            return _randomNum;
        }

        // generate a list of N numbers within a range with a seed (int)
        public static int[] Generate(int seed, int size, int rangeMin, int rangeMax)
        {
            var _random = new Random(seed);
            int[] result = new int[size];

            for (int i = 0; i < size; i++)
            {
                result[i] = _random.Next(rangeMin, rangeMax);

            }

            return result;
        }

        // generate a list of N numbers within a range with a seed (decimal)
        public static decimal[] Generate(decimal seed, int size, decimal rangeMin, decimal rangeMax)
        {
            var _random = new Random((int)seed);
            decimal _randomNum;
            decimal[] result = new decimal[size];

            for (int i = 0; i < size; i++)
            {
                _randomNum = ((decimal)_random.NextDouble() * (rangeMax - rangeMin) + rangeMin);
                _randomNum = (decimal)Math.Round(_randomNum, 2);

                result[i] = _randomNum;

            }

            return result;
        }

        //Select a random item from a list
        public static decimal RandomSelect(decimal seed, int size, int rangeMin, int rangeMax)
        {
            var _random = new Random();
            decimal[] result = new decimal[size];
            result = Generate(seed, size, rangeMin, rangeMax);
            int index = _random.Next(size);
            return (result[index]);
        }

        //Set a seed and randomly select the same value from a list
        public static decimal SeedSelect(decimal seed, int size, int rangeMin, int rangeMax)
        {
            var _random = new Random((int)seed);
            decimal[] result = new decimal[size];
            result = Generate(seed, size, rangeMin, rangeMax);
            int index = _random.Next(size);
            return (result[index]);
        }

        //Select N number of items from a list without a seed
        public static decimal[] NSelect(decimal seed, int size, int rangeMin, int rangeMax, int resultSize)
        {
            var _random = new Random();
            decimal[] result = new decimal[size];
            decimal[] new_result = new decimal[resultSize];
            int index;
            result = Generate(seed, size, rangeMin, rangeMax);

            for (int i = 0; i < resultSize; i++)
            {
                index = _random.Next(size);
                new_result[i] = result[index];
            }



            return (new_result);
        }

        //Select N number of items from a list with a seed
        public static decimal[] NSeedSelect(decimal seed, int size, int rangeMin, int rangeMax, int resultSize)
        {
            decimal[] result = new decimal[size];
            result = Generate(seed, size, rangeMin, rangeMax);
            var _random = new Random((int)seed);
            int index;
            decimal[] new_result = new decimal[resultSize];

            for (int i = 0; i < resultSize; i++)
            {
                index = _random.Next(size);
                new_result[i] = result[index];
            }

            return (new_result);
        }

    }
}