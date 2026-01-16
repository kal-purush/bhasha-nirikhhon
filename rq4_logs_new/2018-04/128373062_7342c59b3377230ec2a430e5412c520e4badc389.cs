using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task4
{
    public class PowerFunction : IFunction
    {
        private double a;
        private uint n;
        private const uint INITIALVALUE = 1;

        public PowerFunction()
        {
            a = INITIALVALUE;
            n = INITIALVALUE;
        }

        public PowerFunction(double _a)
        {
            a = _a;
            n = INITIALVALUE;
        }

        public PowerFunction(double _a, uint _n)
        {
            a = _a;
            n = _n;
        }

        public double FindFunctionValue(double x)
        {
            double functionResult = a * Math.Pow(x, n);
            return functionResult;
        }

        public double FindDerivativeValue(uint derivativeOrder, double x)
        {
            double result = 1;
            while(derivativeOrder != 0)
            {
                result *= n;
                n -= 1;
                derivativeOrder--;
            }

            result *= (a * Math.Pow(x, n));
            return result;
        }

        public int CompareTo(Object obj)
        {
            if (obj == null)
            {
                return 1;
            }
            PowerFunction otherFunction = obj as PowerFunction;
            if (otherFunction != null)
            {
                return this.n.CompareTo(otherFunction.n);
            }
            else
            {
                throw new ArgumentException("Object is not a Power function");
            }
        }

        public Object Clone()
        {
            PowerFunction clone = new PowerFunction(this.a, this.n);
            return clone;
        }

        public void Input()
        {
            Console.WriteLine("Enter coefficient of function: ");
            this.a = Convert.ToDouble(Console.ReadLine());
            Console.WriteLine("Enter power of function: ");
            this.n = Convert.ToUInt32(Console.ReadLine());
        }

        public void Output()
        {
            Console.WriteLine("Function coefficient - {0}; function power - {2};", this.a, this.n);
        }
    }
}