using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLSkaner.IKeyWord
{
    class Identifier : IKeyWords
    {
        static readonly List<string> ReservedWords = new List<string>();

        public Identifier()
        {
            ReservedWords.Add("AVG");
            ReservedWords.Add("SUM");
            ReservedWords.Add("MIN");
            ReservedWords.Add("MAX");

            ReservedWords.Add("AS");

            ReservedWords.Add("TRUE");
            ReservedWords.Add("FALSE");

            ReservedWords.Add("(");
            ReservedWords.Add(",");
            ReservedWords.Add(")");

            ReservedWords.Add(">");
            ReservedWords.Add("<");
            ReservedWords.Add("=");
            ReservedWords.Add(">=");
            ReservedWords.Add("=<");
            ReservedWords.Add("<>");
            ReservedWords.Add("BETWEEN");
            ReservedWords.Add("LIKE");
            ReservedWords.Add("IN");

            ReservedWords.Add("WHERE");
            ReservedWords.Add("DISTINCT");
            ReservedWords.Add("ON");
            ReservedWords.Add("LIMIT");

            ReservedWords.Add("INTEGER");
            ReservedWords.Add("FLOAT");
            ReservedWords.Add("BOOLEAN");
            ReservedWords.Add("CHARACTER");

            ReservedWords.Add("YEAR");
            ReservedWords.Add("MONTH");
            ReservedWords.Add("DAY");

            ReservedWords.Add("JOIN");
            ReservedWords.Add("NATURAL JOIN");
            ReservedWords.Add("INNER");
            ReservedWords.Add("OUTER");

            ReservedWords.Add("AND");
            ReservedWords.Add("OR");
            ReservedWords.Add("NOT");

            ReservedWords.Add("+");
            ReservedWords.Add("-");
            ReservedWords.Add("/");

            ReservedWords.Add("GROUP BY");
            ReservedWords.Add("ORDER BY");
            ReservedWords.Add("ASC");
            ReservedWords.Add("DESC");

            ReservedWords.Add("SELECT");
            ReservedWords.Add("INSERT INTO");
            ReservedWords.Add("UPDATE");
            ReservedWords.Add("DELETE FROM");

            ReservedWords.Add("FROM");
            ReservedWords.Add("SET");
            ReservedWords.Add("VALUES");

            ReservedWords.Add("*");

            ReservedWords.Add("CREATE TABLE");
            ReservedWords.Add("ALTER TABLE");
            ReservedWords.Add("TRUNCATE TABLE");
            ReservedWords.Add("DROP TABLE");

            ReservedWords.Add(" ");
            ReservedWords.Add("\t");
            ReservedWords.Add("\n");
            ReservedWords.Add("\r");
        }

        public bool IsPartialMatch(string input)
        {
            return false;
        }

        public bool IsFullMatch(string input)
        {
            if (MatchingService.IsFullMatch(ReservedWords, input))
                return false;
            if (char.IsDigit(input[0]))
                return false;

            return true;
        }

        public string KeyWordName()
        {
            return "Identifier";
        }

        public string WrapToHtml(string elementToBeWrapped)
        {
            return "<font style=\"color: Yellow\">" + elementToBeWrapped + "</font>";
        }
    }
}