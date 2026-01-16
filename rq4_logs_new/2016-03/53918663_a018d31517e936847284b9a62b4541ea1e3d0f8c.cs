using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace cal_cmd
{
    public static class Calculator
    {
        private static bool isOpt(char x)
        {
            return (x == '+' || x == '-' || x == '*' || x == '/' || x == '(' || x == ')');
        }

        private static bool isNum(char x)
        {
            return (x >= '0' && x <= '9');
        }

        private static int priority(char x)
        {
            if (x == '-' || x == '+')
                return 1;
            else if (x == '*' || x == '/')
                return 2;
            else if (x == '(')
                return 0;
            else
                return -1;
        }

        public static String nifixToPost(String nifixString, out bool hasDemical, out bool error)
        {
            Stack<char> s = new Stack<char>();
            bool hasNum = false;
            bool hasDemicalDuringScan = false;
            bool minusFlag = false;
            long demicalDigitsNum = 1;
            long tmpNum = 0;

            String res = "";
            hasDemical = false;
            error = false;

            for (int i = 0; i < nifixString.Length; i++)
            {
                if (i == 0)
                {
                    if (nifixString[i] == '-')
                    {
                        minusFlag = true;
                        i++;
                    }
                    else if (nifixString[i] == '(') ;
                    else if (isOpt(nifixString[i]))
                    {
                        error = true;
                        return "ERROR-Expression begin with wrong character!";
                    }
                }
                if (isNum(nifixString[i]))
                {
                    hasNum = true;
                    tmpNum = tmpNum * 10 + (nifixString[i] - '0');
                    if (hasDemicalDuringScan)
                        demicalDigitsNum *= 10;
                }
                else if (isOpt(nifixString[i]))
                {
                    if (i > 0)
                    {
                        if (nifixString[i] == '-' && nifixString[i - 1] == '(') ;
                        else if (nifixString[i] == '(' && isOpt(nifixString[i - 1]) && nifixString[i - 1] != ')') ;
                        else if (nifixString[i - 1] == ')') ;
                        else if (isNum(nifixString[i - 1])) ;
                        else
                        {
                            error = true;
                            return "ERROR-Two operators next to each other illegally!";
                        }
                    }
                    if (hasNum)
                    {
                        hasNum = false;
                        //Console.WriteLine("minusFlag = {0}, tmpNum = {1}, hasDemicalDuringScan = {2}, {3}", minusFlag, tmpNum, hasDemicalDuringScan, tmpNum+'0');
                        if (minusFlag)
                        {
                            tmpNum = -tmpNum;
                            minusFlag = false;
                        }
                        res += tmpNum;
                        res += ' ';
                        tmpNum = 0;
                        if (hasDemicalDuringScan)
                        {
                            if (demicalDigitsNum == 1)
                            {
                                error = true;
                                return "ERROR-Digits absent after radix point!-1";
                            }
                            hasDemical = true;
                            res += demicalDigitsNum;
                            res += ' ';
                            demicalDigitsNum = 1;
                            hasDemicalDuringScan = false;
                            res += "/ ";
                        }
                    }
                    if (nifixString[i] == '(')
                    {
                        s.Push(nifixString[i]);
                        if (i+1 < nifixString.Length && nifixString[i+1] == '-')
                        {
                            minusFlag = true;
                            i++;
                        }
                    }
                    else if (nifixString[i] == ')')
                    {
                        if (s.Count == 0)
                        {
                            error = true;
                            return "ERROR-Parentheses unmatched!-1";
                        }
                        char tmpOperator;
                        while(s.Count > 0 && (tmpOperator = s.Pop()) != '(')
                        {
                            res += tmpOperator;
                            res += ' ';
                            if (s.Count == 0)
                            {
                                error = true;
                                return "ERROR-Parentheses unmatched!-2";
                            }
                        }
                    }
                    else
                    {
                        char tmpTop = ' ';
                        bool flag = false;
                        while(true)
                        {
                            if (s.Count == 0)
                                break;
                            tmpTop = s.Pop();
                            if (priority(nifixString[i]) > priority(tmpTop))
                            {
                                flag = true;
                                break;
                            }
                            res += tmpTop;
                            res += ' ';
                        }
                        if (flag)
                            s.Push(tmpTop);
                        s.Push(nifixString[i]);
                    }
                }
                else if (nifixString[i] == '.')
                {
                    if (hasDemicalDuringScan || !hasNum)
                    {
                        error = true;
                        return "ERROR-Too more radix point or has no digits before radix point!";
                    }
                    else
                        hasDemicalDuringScan = true;
                }
                else
                {
                    error = true;
                    return "ERROR-Illegal character!";
                }
            }
            if (hasNum)
            {
                hasNum = false;
                if (minusFlag)
                {
                    tmpNum = -tmpNum;
                    minusFlag = false;
                }
                res += tmpNum;
                res += ' ';
                tmpNum = 0;
                if (hasDemicalDuringScan)
                {
                    if (demicalDigitsNum == 1)
                    {
                        error = true;
                        return "ERROR-Digits absent after radix point!-1";
                    }
                    hasDemical = true;
                    res += demicalDigitsNum;
                    res += ' ';
                    demicalDigitsNum = 1;
                    hasDemicalDuringScan = false;
                    res += "/ ";
                }
            }
            while(s.Count > 0)
            {
                char tmpPop = s.Pop();
                if (tmpPop == '(')
                {
                    error = true;
                    return "ERROR-Parentheses unmatched!-3";
                }
                res += tmpPop;
                res += ' ';
            }
            return res.Substring(0, res.Length-1);
        }
    
        public static String postCalculate(String postString, bool hasDemical, bool error, bool hasPricision, int precision)
        {
            Stack<Number> s = new Stack<Number>();
            if (postString.Length >= 5 && postString.Substring(0, 5).Equals("ERROR"))
                return postString;
            if (error)
                return "ERROR";

            String[] sArray = postString.Split(' ');
            foreach(String i in sArray)
            {
                if (i.Length == 1 && isOpt(i[0]))
                {
                    Number b = new Number(s.Pop());
                    Number a = new Number(s.Pop());
                    switch (i[0])
                    {
                        case '+': s.Push(new Number(a + b)); break;
                        case '-': s.Push(new Number(a - b)); break;
                        case '*': s.Push(new Number(a * b)); break;
                        case '/': s.Push(new Number(a / b)); break;
                        default: return "ERROR-Illegal character";
                    }
                }
                else
                    s.Push(new Number(Convert.ToInt64(i)));
            }
            if (s.Count > 1)
                return "ERROR";
            if (hasPricision)
                return s.Pop().toString(precision);
            else if (hasDemical)
                return s.Pop().toString(hasDemical);
            else
                return s.Pop().toString();
        }
    }
}