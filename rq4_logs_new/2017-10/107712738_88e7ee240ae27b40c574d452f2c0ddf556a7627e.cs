using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    public class BasicMath : IBasicMath
    {
        public float Add(float A, float B)
        {
            return A + B;
        }

        public float Divide(float A, float B)
        {
            return A / B;
        }

        public float Multiply(float A, float B)
        {
            return A * B;
        }

        public float Subtract(float A, float B)
        {
            return A - B;
        }
    }
}