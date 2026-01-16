using System;
using System.CodeDom.Compiler;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BinaryRepresentation;

namespace MOC_Tema1_1
{
    public class HillClimbing
    {
        //bazinele de atractie
        private double[] firstImprovement = new double[32];
        private double[] bestImprovement = new double[32];

        private double TestFunction(BinaryRepresentation number)
        {
            var x = number.ConvertToDouble();
            if (x > 31 || x < 0)
            {
                throw new ArgumentOutOfRangeException();
            }

            double sum = 100;
            sum += Math.Pow(x, 3);
            sum -= (60 * x * x);
            sum += 900 * x;

            return sum;
        }


        public void BestImprovement()
        {
            
        }

        public void FirstImprovement()
        {
            
        }

    }

    public class BinaryRepresentation
    {
        private BitArray _numberBitArray;
        private const int NUMBER_OF_BITS = 5;

        public BinaryRepresentation(BitArray source)
        {
            _numberBitArray = new BitArray(NUMBER_OF_BITS);
            for (int i = 0; i < NUMBER_OF_BITS; i++)
            {
                _numberBitArray[i] = source[i];
            }
        }

        public double ConvertToDouble()
        {
            int sum = 0;
            for (int convertPos = 0; convertPos < NUMBER_OF_BITS; convertPos++)
            {
                if (_numberBitArray[convertPos])
                {
                    sum += (int)Math.Pow(2, convertPos);
                }
            }
            return sum;
        }

    }
}