using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OpenTK.Mathematics;

namespace Minecraft.System
{
    internal class NoiseMap
    {
        private FastNoiseLite Noise { get; set; }

        internal List<Vector2> Splines { get; set; } = new List<Vector2>();

        internal FastNoiseLite.NoiseType NoiseType 
        { 
            get => noiseType; 
            set { noiseType = value; Noise.SetNoiseType(value); } 
        }
        internal FastNoiseLite.FractalType FractalType
        {
            get => fractalType;
            set { fractalType = value; Noise.SetFractalType(value); }
        }
        internal int NumFractalOcaves 
        { 
            get => numFractalOcaves;
            set { numFractalOcaves = value; Noise.SetFractalOctaves(value); }
        }
        internal float Frequency 
        { 
            get => frequency;
            set { frequency = value; Noise.SetFrequency(value); }
        }
        internal int Seed
        {
            get => seed;
            set { seed = value; Noise.SetSeed(value); }
        }

        private FastNoiseLite.NoiseType noiseType;
        private FastNoiseLite.FractalType fractalType;
        private int numFractalOcaves;
        private float frequency;
        private int seed;

        internal NoiseMap(int seed, float frequency, FastNoiseLite.NoiseType noiseType = FastNoiseLite.NoiseType.Perlin, 
            FastNoiseLite.FractalType fractalType = FastNoiseLite.FractalType.FBm, int numFractalOcaves = 4)
        {
            this.Noise = new FastNoiseLite(seed);
            this.NoiseType = noiseType;
            this.FractalType = fractalType;
            this.NumFractalOcaves = numFractalOcaves;
            this.Frequency = frequency;
            this.Seed = seed;
        }

        internal void CreateSpline(Vector2 position)
        {
            this.Splines.Add(position);
            this.Splines.Sort((spline1, spline2) => 
            {
                if (spline1.X < spline2.X)
                    return -1;
                if (spline1.X == spline2.X)
                    return 0;
                return 1;
            });
        }

        internal void RemoveSpline(Vector2 position)
        {
            this.Splines.Remove(position);
            this.Splines.Sort((spline1, spline2) =>
            {
                if (spline1.X < spline2.X)
                    return -1;
                if (spline1.X == spline2.X)
                    return 0;
                return 1;
            });
        }

        /// <summary>
        /// Return a map noised value from 0, 1
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <returns></returns>
        internal float GetMapedNoiseValue(int x, int y)
        {
            float noiseData = this.Noise.GetNoise(x, y);
            // Convert from -1, 1 to 0, 2 then to 0, 1
            noiseData = (noiseData + 1) / 2;

            Vector2 spline1 = this.Splines.ValueOfLesser(noiseData);
            Vector2 spline2 = this.Splines.ValueOfGreater(noiseData);

            float data = (((spline2.Y - spline1.Y) / (spline2.X - spline1.X)) * (noiseData - spline1.X)) + spline1.Y;
            return data;
        }

        /// <summary>
        /// Returns maped data from 0, 1
        /// </summary>
        /// <param name="x">Staring X</param>
        /// <param name="y">Starting Y</param>
        /// <param name="length">How much data to copy in each dimension</param>
        /// <returns></returns>
        internal float[,] GetMapedNoiseData(int x, int y, int length)
        {
            float[,] data = new float[length, length];
            for (int i = 0; i < length; i++)
            {
                for (int j = 0; j < length; j++)
                {
                    data[i, j] = GetMapedNoiseValue(x + i, y + j);
                }
            }
            return data;
        }

        /// <summary>
        /// Converts mapedData to a new intiger scale
        /// </summary>
        /// <param name="mapedData">Data from 0 to 1</param>
        /// <param name="min">Output minimum</param>
        /// <param name="max">Output maximum</param>
        /// <returns></returns>
        internal int[,] ConvertMapedDataToIntScale(float[,] mapedData, int min, int max)
        {
            int[,] heightData = new int[mapedData.GetLength(0), mapedData.GetLength(1)];

            for (int x = 0; x < mapedData.GetLength(0); x++)
            {
                for (int y = 0; y < mapedData.GetLength(1); y++)
                {
                    heightData[x, y] = (int)Map(0, 1, min, max, mapedData[x, y]);
                }
            }
            return heightData;
        }

        /// <summary>
        /// Maps data form one scale to a another
        /// </summary>
        /// <param name="inputMin">Input scale minimum</param>
        /// <param name="inputMax">Input scale maximum</param>
        /// <param name="outputMin">Output scale minimum</param>
        /// <param name="outputMax">Output scale maximum</param>
        /// <param name="value">Value in the input scale</param>
        /// <returns></returns>
        private static float Map(float inputMin, float inputMax, float outputMin, float outputMax, float value)
        {
            // Calculate the input range and output range
            float inputRange = inputMax - inputMin;
            float outputRange = outputMax - outputMin;

            // Calculate the normalized value of the input
            float normalizedValue = (value - inputMin) / inputRange;

            // Scale the normalized value to the output range
            float scaledValue = outputMin + normalizedValue * outputRange;

            // Return the scaled value
            return scaledValue;
        }
    }
}