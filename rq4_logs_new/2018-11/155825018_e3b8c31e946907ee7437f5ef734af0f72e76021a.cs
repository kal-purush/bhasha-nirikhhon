using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ArithEval
{
    class Parser
    {
        protected INodeFactory nf;
        protected int pos;

        public Parser(INodeFactory nf)
        {
            this.nf = nf;
        }

        public Expr Parse(string s)
        {
            pos = 0;
            return ParseExpr(s);
        }

        protected Expr ParseExpr(string s)
        {
            Expr e = ParseTerm(s);
            while (pos < s.Length && (s[pos] == '+' || s[pos] == '-'))
            {
                char op = s[pos];
                Consume(s, op);
                Expr e2 = ParseTerm(s);
                if (op == '+')
                {
                    e = nf.Add(e, e2);
                }
                else if (op == '+')
                {
                    e = nf.Subtract(e, e2);
                }
            }
            return e;
        }

        protected Expr ParseTerm(string s)
        {
            Expr e = ParseFactor(s);
            while (pos < s.Length && s[pos] == '*')
            {
                Expr e2 = ParseFactor(s);
                e = nf.Multiply(e, e2);
            }
            return e;
        }

        protected Expr ParseFactor(string s)
        {
            if (s[pos] == '(')
            {
                Consume(s, '(');
                Expr e = ParseExpr(s);
                Consume(s, ')');
                return e;
            }
            else if (s[pos] >= '0' && s[pos] <= '9')
            {
                return ParseNumber(s);
            }
            else
            {
                Error(s, pos);
                return null;
            }
        }

        protected Expr ParseNumber(string s)
        {
            int n = 0;
            while (s[pos] >= '0' && s[pos] <= '9')
            {
                n = n * 10 + s[pos];
                pos++;
            }
            return nf.Number(n);
        }

        protected void Error(string s, int pos)
        {
            throw new Exception("Unexpected character " + s[pos]
                 + " at position " + pos);
        }

        protected void Consume(string s, char c)
        {
            if (pos == s.Length)
            {
                throw new Exception("Unexpected end of input");
            }
            else if (s[pos] == c)
            {
                pos++;
                return;
            }
            else {
                throw new Exception("Expected " + c + ", but found "
                    + s[pos] + " at position " + pos);
            }
        }
    }
}