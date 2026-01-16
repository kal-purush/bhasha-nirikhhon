using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CalculatorNamespace
{
    public class Calculator
    {
        private IStack stack;

        /// <summary>
        /// constructor for calculator
        /// </summary>
        /// <param name="tmp"></param>
        public Calculator(IStack tmp)
        {
            stack = tmp;
        }

        /// <summary>
        /// add element in stack
        /// </summary>
        /// <param name="value"></param>
        public void AddElement(int value)
        {
            stack.Push(value);
        }

        /// <summary>
        /// addition of two elements
        /// </summary>
        public void Addition()
        {
            int result = stack.Pop() + stack.Pop();
            stack.Push(result);
        }

        /// <summary>
        /// subtraction of elements
        /// </summary>
        public void Subtraction()
        {
            int result = stack.Pop() - stack.Pop();
            stack.Push(result);
        }

        /// <summary>
        /// multiply of elements
        /// </summary>
        public void Multy()
        {
            int result = stack.Pop() * stack.Pop();
            stack.Push(result);
        }

        /// <summary>
        /// divide of elements
        /// </summary>
        public void Divide()
        {
            int result = stack.Pop() / stack.Pop();
            stack.Push(result);
        }

        /// <summary>
        /// print result of operation
        /// </summary>
        public int Result()
        {
            return stack.Pop();
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            IStack tmp;
            //tmp = new List();
            tmp = new Stack();
            Calculator calc = new Calculator(tmp);
            calc.AddElement(5);
            calc.AddElement(9);
            calc.Divide();
            Console.WriteLine(calc.Result());
        }
    }
}