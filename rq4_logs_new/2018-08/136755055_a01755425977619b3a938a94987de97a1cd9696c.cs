using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace switchStatement
{
    public abstract class AbstractClass
    {
        private int _paramOne;
        private int _paramTwo;
        protected double addResult;

        public void AdditionResult()
        {
            Console.WriteLine("Addition of two number is {0} ", addResult);
        }

        public abstract void AddMe(int numOne, int numTwo);

        public abstract void AddMe(double numOne, double numTwo);

        public abstract void AddMe(int numOne, double numTwo);




    }
}