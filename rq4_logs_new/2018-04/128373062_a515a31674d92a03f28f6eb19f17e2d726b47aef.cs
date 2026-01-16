using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task4
{
    public class Task
    {
        private static IFunction[] InputFunctions(IFunction[] functions)
        {
            for (uint i = 0; i < functions.Length; i++)
            {
                string functionType = "";
                while (functionType!= "trig" && functionType!= "pow")
                {
                    Console.Write("Enter correct function type (trig - trigonometric, pow - power): ");
                    functionType = Console.ReadLine();
                }

                switch (functionType)
                {
                    case "trig":
                        functions[i] = new TrigonometricFunction();
                        break;
                    case "pow":
                        functions[i] = new PowerFunction();
                        break;
                    default:
                        throw new ArgumentException("Invalid function type");
                }
                functions[i].Input();
            }

            return functions;
        }

        private static void OutputFunctions(IFunction[] functions)
        {
            Console.WriteLine("______________");
            foreach (var item in functions)
            {
                item.Output();
            }
        }
        private static double FindFunctionValue(double x, IFunction[] functions)
        {
            double result = 0;
            foreach (var function in functions)
            {
                result += function.FindFunctionValue(x);
            }
            return result;
        }

        private static double FindDerivativeValue(uint derivativeOrder, double x, IFunction[] functions)
        {
            double result = 0;
            foreach (var function in functions)
            {
                result += function.FindDerivativeValue(derivativeOrder, x);
            }
            return result;
        }

        private static IFunction[] CloneArray(IFunction[] arrayToClone)
        {
            IFunction[] clonedArray = new IFunction[arrayToClone.Length];
            uint functionIndex = 0;
            foreach(var function in arrayToClone)
            {
                clonedArray[functionIndex] = function.Clone() as IFunction;
                functionIndex++;
            }
            return clonedArray;
        }

        public static void SwapFunctions(ref IFunction f1, ref IFunction f2)
        {
            IFunction temp = f1;
            f1 = f2;
            f2 = temp;
        }

        public static IFunction[] SortFunctionsByValueInX(IFunction[] functions, double x)
        {
            for (uint i = 0; i < functions.Length; i++)
            {
                for (uint j = 0; j < functions.Length - 1; j++)
                {
                    if (functions[j].FindFunctionValue(x).CompareTo(functions[j + 1].FindFunctionValue(x)) > 0)
                    {
                        SwapFunctions(ref functions[j], ref functions[j + 1]);
                    }
                }
            }

            return functions;
        }
        public static void Execute()
        {
            Console.WriteLine("Enter amount of functions in your expression: ");
            if (!uint.TryParse(Console.ReadLine(), out uint amountOfFunctions))
            {
                throw new ArgumentException("Impossible amount of functions");
            }

            IFunction[] functions = new IFunction[amountOfFunctions];
            functions = InputFunctions(functions);
            Console.WriteLine("Unsorted array of functions:");
            OutputFunctions(functions);
            Console.WriteLine("______________");
            Console.Write("Enter x to find value of your expression:");
            double x = Convert.ToDouble(Console.ReadLine());
            IFunction[] clonedArrayOfFunctions = CloneArray(functions);
            SortFunctionsByValueInX(clonedArrayOfFunctions, x);
            Console.WriteLine("Cloned array of functions, sorted in ascending order:");
            OutputFunctions(clonedArrayOfFunctions);
            Console.WriteLine("______________");
            Console.WriteLine("Expression value in point x: {0}", FindFunctionValue(x, functions));
            Console.WriteLine("Value of first derivative in point x: {0}", FindDerivativeValue(1, x, functions));
            Console.WriteLine("Value of second derivative in point x: {0}", FindDerivativeValue(2, x, functions));
            Console.WriteLine("Value of third derivative in point x: {0}", FindDerivativeValue(3, x, functions));

        }
    }
}