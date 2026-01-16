using System;
using System.Collections.Generic;
using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

namespace Homework
{
    public class BenchmarkClass
    {
        private const int SIZE = 1_000_000;
        private PointClassFloat[] pointClassFloatArray = new PointClassFloat[SIZE];
        private PointStructFloat[] pointStructFloatArray = new PointStructFloat[SIZE];
        private PointStructDouble[] pointStructDoubleArray = new PointStructDouble[SIZE];

        public BenchmarkClass()
        {
            Random rnd = new Random();
            for (int i = 0; i< SIZE; i++)
            {
                int x = rnd.Next(1,100);
                int y = rnd.Next(1, 100);
                pointClassFloatArray[i] = new PointClassFloat(x, y);
                pointStructFloatArray[i] = new PointStructFloat(x, y);
                pointStructDoubleArray[i] = new PointStructDouble(x, y);
            }
        }

        [Benchmark]
        public void BechmarkClassFloat()
        {
            for(int i = 0; i<SIZE-1; i++)
                PointClassFloat.PointDistance(pointClassFloatArray[i], pointClassFloatArray[i+1]);
        }

        [Benchmark]
        public void BechmarkStructFloat()
        {
            for (int i = 0; i < SIZE - 1; i++)
                PointStructFloat.PointDistance(pointStructFloatArray[i], pointStructFloatArray[i + 1]);
        }
        
        [Benchmark]
        public void BechmarkStructDouble()
        {
            for (int i = 0; i < SIZE - 1; i++)
                PointStructDouble.PointDistance(pointStructDoubleArray[i], pointStructDoubleArray[i + 1]);
        }
        
        [Benchmark]
        public void BechmarkStructShortFloat()
        {
            for (int i = 0; i < SIZE - 1; i++)
                PointStructFloat.PointDistanceShort(pointStructFloatArray[i], pointStructFloatArray[i + 1]);
        }
    }
}