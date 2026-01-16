using System;
using System.Collections.Generic;
using System.Text;

namespace HealthMgr
{
   public class BmiCalculator
    {
        public int Weight { get; set; }
        public int Height { get; set; }
        public float BMI
        {
            get
            {
                return Calculate();
            }
        }


        public float Calculate()
        {
            float result = 0;
            //將float改成int試試看
            float height = (int)Height / 100;
            result = Weight / (height * height);

            return result;
        }
    }
}