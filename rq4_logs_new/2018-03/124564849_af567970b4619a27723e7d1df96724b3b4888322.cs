using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLSkaner
{
    interface KeyWords
    {
        //List<string> matchingPatterns();
        //string getColor();
        bool isPartialMatch(string input);
        bool isFullMatch(string input);
    }

    class MathematicalOperators : KeyWords
    {
        /*public List<string> matchingPatterns()
        {
            return new List<string>
            {
                "+",
                "-",
                "/"
            };
        }*/

        /*public string getColor()
        {
            return "";
        }*/

        public bool isPartialMatch(string input)
        {
            return false;
        }

        public bool isFullMatch(string input)
        {
            if (input.Equals("+"))
                return true;
            if (input.Equals("-"))
                return true;
            if (input.Equals("/"))
                return true;
            return false;
        }
    }

    class FoundKeyWord
    {
        private string foundPattern;
        private KeyWords keyWordType;

        FoundKeyWord(string foundPattern, KeyWords keyWordType)
        {
            this.foundPattern = foundPattern;
            this.keyWordType = keyWordType;
        }
    }

    class Skaner
    {
        List<FoundKeyWord> fullMatchList = new List<FoundKeyWord>();
        //List<FoundKeyWord> partialMatchList = new List<FoundKeyWord>();
        List<KeyWords> allPossibleKeywordsList = new List<KeyWords>{new MathematicalOperators()};
        private string Buffor;
        private string Input;

        bool anyPatilMatch(string input)
        {
            foreach (var keyWord in allPossibleKeywordsList)
            {
                if (keyWord.isPartialMatch(input)) return true;
            }
            return false;
        }

        bool anyFullMatch(string input)
        {
            foreach (var keyWord in allPossibleKeywordsList)
            {
                if (keyWord.isFullMatch(input)) return true;
            }
            return false;
        }

        FoundKeyWord iterate(string input)
        {
            if(input == null) 
          Buffor = input[0]  
        }

        List<FoundKeyWord> DoScan()
    }

    class Program
    {
        static void Main(string[] args)
        {
        }
    }
}