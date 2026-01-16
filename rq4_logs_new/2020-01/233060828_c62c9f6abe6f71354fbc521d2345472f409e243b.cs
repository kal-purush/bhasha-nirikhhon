using System;
using Helper;

namespace StatsCalc_IO
{
    public interface IMainStatCalculator
    {
        public double meanv(double[] arrayvalues);
        public double standarddeviationv(double[] arrayvalues);
        public double modev(double[] arrayvalues);
        public double medianv(double[] arrayvalues);
        public double variancev(double[] arrayvalues);
    }
    public class MainStatCalculator : IMean,IMedian,IMode,IStandardDeviation,IVariance
    {
        private readonly int DecimalPlace;
        private readonly Mean mn = new Mean();
        private readonly Variance vr= new Variance();
        private readonly Mode md = new Mode();
        private readonly Median mdn = new Median();
        private readonly StandardDeviation sd = new StandardDeviation();
        private dynamic result;


        public dynamic Result
        {

            get
            {
             string type = result.GetType().ToString();
             if (type == "System.Int32")
            {
             return result;
            }
            else
            {
            return HelperClass.RoundNumberToDecimalPlaces(DecimalPlace, result);
            }
            }
            set
            {
            result = value;
            }
        }

        public MainStatCalculator(int decimalPlace = 2)
        {
         DecimalPlace = decimalPlace;
        }

        public double meanv(double[] arrayvalues)
        {
            Result =mn.meanv(arrayvalues); 
            return Result;
        }

        public double modev(double[] arrayvalues)
        {
            Result =md.modev(arrayvalues); 
            return Result;
        }

        public double standarddeviationv(double[] arrayvalues)
        {
            Result = sd.StandardDeviationv(arrayvalues); 
            return Result;
        }

        public double medianv(double[] arrayvalues)
        {
            Result = mdn.medianv(arrayvalues);
            return Result;
        }

        public double variancev(double[] arrayvalues)
        {
            Result = vr.variancev(arrayvalues);
            return Result;
        }

        public double StandardDeviationv(double[] arrayvalues)
        {
            throw new NotImplementedException();
        }
    }
}