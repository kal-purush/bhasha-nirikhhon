using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;

namespace inc
{
    class Program
    {
        enum OPERATOR { NOOP, ADD, SUB, DIV, MULT };

        static string DoParse(string str, int val)
        {
            int i = 0;
            bool inParse = false;
            bool formattingResNum = false;
            bool lookingForOperator = false;
            OPERATOR op = OPERATOR.NOOP;
            string operand = "";
            string stringOut = "";
            int res = 0;
            string numFmt = "{0,0:D0}";
            string zeroString = "";
            string operators = "0+-/*";
            string strIn = str + '\0'; // ensures that last character is parsed

            for (i = 0; i < strIn.Length; i++)
            {
                if (formattingResNum)
                {
                    if (strIn[i] == '0')
                        zeroString += '0';
                    else
                    {
                        numFmt = string.Format("{{0:{0}}}", zeroString);
                        zeroString = "";
                        formattingResNum = false;
                        lookingForOperator = true;
                    }
                }

                if (inParse)
                {
                    if ((strIn[i] >= '0') && (strIn[i] <= '9'))
                        operand += strIn[i];
                    else
                    {
                        switch (op)
                        {
                            case OPERATOR.NOOP:
                                res = val;
                                break;
                            case OPERATOR.ADD:
                                res = val + int.Parse(operand);
                                break;
                            case OPERATOR.SUB:
                                res = val - int.Parse(operand);
                                break;
                            case OPERATOR.DIV:
                                res = val / int.Parse(operand);
                                break;
                            case OPERATOR.MULT:
                                res = val * int.Parse(operand);
                                break;
                        }
                        stringOut += string.Format(numFmt, res);
                        operand = "";
                        inParse = false;
                    }
                }
                else if (lookingForOperator)
                {
                    if (operators.IndexOf(strIn[i]) > -1)
                    {
                        if (strIn[i] == '0')
                        {
                            formattingResNum = true;
                            zeroString = "0";
                            lookingForOperator = false;
                        }
                        else
                        {
                            if (strIn[i] == '+')
                                op = OPERATOR.ADD;
                            else if (strIn[i] == '-')
                                op = OPERATOR.SUB;
                            else if (strIn[i] == '*')
                                op = OPERATOR.MULT;
                            else if (strIn[i] == '/')
                                op = OPERATOR.DIV;
                            inParse = true;
                            lookingForOperator = false;
                        }
                    }
                    else
                    {
                        stringOut += string.Format(numFmt, val);
                        lookingForOperator = false;
                    }
                }

                if (strIn[i] == '@')
                {
                    lookingForOperator = true;
                }
                else if ((!formattingResNum) &&
                         (!inParse) &&
                         (!lookingForOperator) &&
                         (strIn[i] != '\0'))
                    stringOut += strIn[i];
            }

            return stringOut;
        }
        static void Usage()
        {
            Console.WriteLine("Usage: inc start# end# step# cmd - where any @ is replaced with sequence # - step must move start towards end!");
        }
        static bool DoWeContinue(int start, int end, int i)
        {
            if (end >= start)
                return (i <= end);
            else
                return (i >= end);
        }

        static void Main(string[] args)
        {
            int start = 0;
            int end = 0;
            int step = 1;
            string formatString = "";
            int i = 0;

            if (args.Length < 4)
            {
                Usage();
                return;
            }
            for (i = 0; i < args.Length; i++)
            {
                if (i == 0)
                    start = int.Parse(args[i]);
                else if (i == 1)
                    end = int.Parse(args[i]);
                else if (i == 2)
                    step = int.Parse(args[i]);
                else
                {
                    formatString += args[i];
                    if (i < (args.Length - 1))
                        formatString += " ";
                }
            }

            if (((start > end) && (step > 0)) ||
                ((start < end) && (step < 0)) ||
                ((start != end) && (step == 0)))
            {
                Usage();
                return;
            }

            for (i = start; DoWeContinue (start, end, i); i += step)
            {
                string parsedString = DoParse(formatString, i);
                Console.WriteLine("{0}:{1}", i, parsedString);
                string cmd = string.Format("/c {0}", parsedString);
                try
                {
                    System.Diagnostics.Process cmdProcess = new System.Diagnostics.Process();
                    cmdProcess.StartInfo.UseShellExecute = false;
                    cmdProcess.StartInfo.Arguments = cmd;
                    cmdProcess.StartInfo.FileName = "cmd.exe";
                    cmdProcess.Start();
                    cmdProcess.WaitForExit();
                    cmdProcess.Dispose();
                    cmdProcess.Close();
                }
                catch (System.ComponentModel.Win32Exception err)
                {
                    Console.WriteLine("\"" + cmd + "\" " + err.Message);
                }
            }
        }
    }
}