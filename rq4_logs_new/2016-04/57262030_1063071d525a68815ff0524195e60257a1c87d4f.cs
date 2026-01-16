using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace at.jku.ssw.Coco
{
    #region Parser
    public partial class Parser
    {
        const bool _T = true;
        const bool _x = false;

        Token t;    // last recognized token
        Token la;   // lookahead token
        int _errDist = Constants.MINIMUM_ERROR_DISTANCE;

        public Parser(Scanner scanner)
        {
            Scanner = scanner;
        }

        void SynErr(int n)
        {
            if (_errDist >= Constants.MINIMUM_ERROR_DISTANCE)
                Errors.SynErr(la.line, la.col, n);
            _errDist = 0;
        }

        public void SemErr(string msg)
        {
            if (_errDist >= Constants.MINIMUM_ERROR_DISTANCE)
                Errors.SemErr(t.line, t.col, msg);
            _errDist = 0;
        }

        void Expect(int n)
        {
            if (la.kind == n)
                Get();
            else
                SynErr(n);
        }

        bool StartOf(int s)
        {
            return set[s, la.kind];
        }

        void ExpectWeak(int n, int follow)
        {
            if (la.kind == n)
                Get();
            else
            {
                SynErr(n);
                while (!StartOf(follow))
                    Get();
            }
        }

        bool WeakSeparator(int n, int syFol, int repFol)
        {
            int kind = la.kind;
            if (kind == n)
            {
                Get();
                return true;
            }
            else if (StartOf(repFol))
            {
                return false;
            }
            else
            {
                SynErr(n);
                while (!(set[syFol, kind] || set[repFol, kind] || set[0, kind]))
                {
                    Get();
                    kind = la.kind;
                }
                return StartOf(syFol);
            }
        }

        public Scanner Scanner { get; private set; }
        public Errors Errors { get; private set; } = new Errors();
    }
    #endregion Parser

    #region Errors
    public partial class Errors
    {
        const string ERR_MSG_FORMAT = "-- line {0} col {1}: {2}"; // 0=line, 1=column, 2=text


        public virtual void SemErr(int line, int col, string s)
        {
            WriteLine(ERR_MSG_FORMAT, line, col, s);
            ErrorCount++;
        }

        public virtual void SemErr(string s)
        {
            WriteLine(s);
            ErrorCount++;
        }

        public virtual void Warning(int line, int col, string s)
            => WriteLine(ERR_MSG_FORMAT, line, col, s);

        public virtual void Warning(string s)
            => WriteLine(s);

        void WriteLine(string format, params object[] args)
            => Debug.WriteLine(format, args);

        // The number of errors generated
        public int ErrorCount { get; private set; } = 0;
    }
    #endregion Errors

    #region FatalError
    public class FatalError : Exception
    {
        public FatalError(string m) : base(m) { }
    }
    #endregion FatalError
}