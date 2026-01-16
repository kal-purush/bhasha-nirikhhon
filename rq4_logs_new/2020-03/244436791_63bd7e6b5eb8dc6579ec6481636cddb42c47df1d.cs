using System;
using basicCalculator;
using Microsoft.CSharp;
using MathNet;
using RandomGen;

namespace StatisticsCalculator
{
    public class PopulationStats : basicCalculators
    {

        DescriptiveStats statsCalc = new DescriptiveStats();

        public decimal SimpleRandomSample(dynamic sample, dynamic population)
        {

            decimal samplesize = Quotient(sample, population);
            return samplesize;

        }

        public decimal SystematicSampling(dynamic sample, dynamic population)
        {

            decimal k = Quotient(population, sample);
            return k;

        }

    }
}