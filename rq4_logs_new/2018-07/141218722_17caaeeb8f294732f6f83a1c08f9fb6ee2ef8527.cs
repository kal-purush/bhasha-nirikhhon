using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassesCalculator
{
    public class Calc
    {
        //****Declare Fields - None ****
        //****Declare Properties ****
        public double Num1 { get; set; }

        public double Num2 { get; set; }

        //show the symbol in the listbox
        public string SelectedSymbol { get; set; }

        /// <summary>
        /// Add two numbers
        /// </summary>
        public double Add(double one, double two)
        {
            return one + two;
        }

        /// <summary>
        /// Subtract two numbers
        /// </summary>
        public double Subtract(double one, double two)
        {
            return one - two;
        }

        /// <summary>
        /// Divide two numbers
        /// </summary>
        public double Divide(double one, double two)
        {
            return one / two;

        }

        /// Multiply two numbers
       
        public double Multiply(double one, double two)
        {
            return one * two;
        }

        /// <summary>
        /// Load the symbols to the ComboBox
        /// </summary>
        public string[] LoadSymbols()
        {
            //make an array of symbols
            String[] ComboBoxItems = new[] {"Choose an operation", "*", "-", "+", "/"};
            return ComboBoxItems;
        }

        /// <summary>
        /// Input the Symbol and select the calculation
        /// </summary>
        public double SelectOperation()
        {
            double Answer = 0;
            switch (SelectedSymbol)
            {
                case "*":
                    Answer = Multiply(Num1, Num2);
                    break;
                case "-":
                    Answer = Subtract(Num1, Num2);
                    break;
                case "+":
                    Answer = Add(Num1, Num2);
                    break;
                case "/":
                    Answer = Divide(Num1, Num2);
                    break;
            }

            return Answer;
        }
    }
}