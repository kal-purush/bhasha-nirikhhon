using System;
using System.Collections.Generic;

namespace NPVEngine
{
    public class CalculateSetOfNPVRequest: IValidateInputs
    {
        public List<double> CashFlow { get; set; }
        public double UpperBoundDiscountRate { get; set; }
        public double LowerBoundDiscountRate { get; set; }
        public double Increment { get; set; }
        public double InitialCost { get; set; }

        public void ValidateInputs()
        {
            if(InitialCost>0)
            {
                throw new ArgumentException("Initial Cost must be in negative value");
            }
        }
    }
}