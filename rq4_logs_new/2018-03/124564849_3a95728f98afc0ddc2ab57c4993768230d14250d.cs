using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLSkaner
{
    class Comparator : IKeyWords
    {
        public bool IsFullMatch(string input)
        {
            if ((input == ">") || (input == "<") || (input == "="))
                return true;
            return false;
        }

        public bool IsPartialMatch(string input)
        {
            throw new NotImplementedException();
        }

        public string KeyWordName()
        {
            return "Comparator";
        }
    }
}