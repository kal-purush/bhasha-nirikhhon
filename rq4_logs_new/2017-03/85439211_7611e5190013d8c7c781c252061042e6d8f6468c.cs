using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Calculator
{
    class Analyzer
    {
        public Queue<string> digits;
        public Stack<string> symbols;
        public List<string> values;
        public Calculus calc;

        public Analyzer() 
        { 
            this.digits = new Queue<string>(); 
            this.symbols = new Stack<string>();
            this.values = new List<string>();
            calc = new Calculus();
        }

        public Result Calculate (string equation)
        {
            ClearCollections();

	        if (equation[0] == '@')
		        return new Result(0, "");
            if (CheckForSymbol(equation[equation.Count() - 2].ToString()) && 
                equation[equation.Count() - 2].ToString() != ")")
                return new Result(0, "Error 05. Unfinished expression!");
	        
		    Parse(equation);
            EmptyContainers();
            return FindResult();
        }

        public Result MemoryCalculate(Double a, Double b)
        {
            return calc.Run(a, b, "+");
        }

        private void Parse (string equation)
        {
	        equation = equation.Replace(" ", "");
            string number;
            int ctr;

            for(int i = 0, j = 1, size = equation.Length; i < size; i += j)
            {
                j = 1;
                number = "";

                if (CheckForDigit(equation[i].ToString()) == true)
                {
                    ctr = j;
                    number += equation[i].ToString();
                    if (i + ctr < size)
                    {
                        while (CheckForDigit(equation[i + ctr].ToString()) != false)
                        {
                            number += equation[i + ctr].ToString();
                            ctr++;
                        }
                    }
                    j = ctr;
                    digits.Enqueue(number);
                }
                else
                {
                    if (equation[i] == '(')
                        symbols.Push(equation[i].ToString());
                    else if (equation[i] == '+' || equation[i] == '-')
                    {
                        if (symbols.Count == 0)
                            symbols.Push(equation[i].ToString());
                        else
                        {
                            string tmp1 = symbols.Peek();
                            if (tmp1 == "-" || tmp1 == "*" || tmp1 == "/" || tmp1 == "+" || tmp1 == "%")
                            {
                                tmp1 = symbols.Pop();
                                digits.Enqueue(tmp1);
                                symbols.Push(equation[i].ToString());
                            }
                            else
                                symbols.Push(equation[i].ToString());
                        }
                    }
                    else if (equation[i] == '*' || equation[i] == '/' || equation[i] == '%')
                    {
                        if (symbols.Count == 0)
                            symbols.Push(equation[i].ToString());
                        else
                        {
                            string tmp2 = symbols.Peek();
                            if (tmp2 == "*" || tmp2 == "/" || tmp2 == "%")
                            {
                                tmp2 = symbols.Pop();
                                digits.Enqueue(tmp2);
                                symbols.Push(equation[i].ToString());
                            }
                            else
                                symbols.Push(equation[i].ToString());
                        }
                    }
                    else if (equation[i] == ')')
                    {
                        string tmp3, tmp4;
                        tmp3 = symbols.Pop();
                        digits.Enqueue(tmp3);
                        tmp4 = symbols.Pop();
                        if ("(" != tmp4)
                            digits.Enqueue(tmp4);
                    }
                }
            }
        }

        private void EmptyContainers ()
        {
            if (symbols.Count > 0)
                while (symbols.Count != 0)
                    digits.Enqueue(symbols.Pop());
            
            while (digits.Count != 0)
            {
                string tmp = digits.Dequeue();
                if(tmp != null)
                    values.Add(tmp);
            }
        }

        private Result FindResult()
        {
            if(values.Count > 30)
                return new Result(0, "Error 08. Too many operands!");

            string tmp;
            int top = -1;           
            Result rslt;
            Double[] results = new Double[50];
 
            for (int i = 0; i < values.Count(); i++)
                if ((CheckForSymbol(values[i].ToString()) == false)) 
                {
                    tmp = values[i].ToString();
                    results[++top] = Convert.ToDouble(tmp);
                }
                else
                {
                    if (results[top] == 0)
                        return new Result(0, "");
                    else
                    {
                        rslt = calc.Run(results[top - 1], results[top], values[i].ToString());
                        if (rslt.errors == String.Empty)
                        {
                            results[top - 1] = rslt.rslt;
                            top--;
                        }
                        else
                            return rslt;             
                    }
                }

            return new Result(results[0], "");
        }

        private bool CheckForDigit(string value)
        {
            if (value == "0" || value == "1" || value == "2" || value == "3" || value == "4" ||
                value == "5" || value == "6" || value == "7" || value == "8" || value == "9")
                return true;
            else
                return false;
        }

        private bool CheckForSymbol (string value)
        {
            if (value == "+" || value == "-" || value == "*" || value == "/" ||
                value == "(" || value == ")" || value == "%")
		        return true;
	        else
		        return false;
        }

        private void ClearCollections()
        {
            digits.Clear();
            symbols.Clear();
            values.Clear();
        }
    }
}