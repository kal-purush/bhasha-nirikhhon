using System.Collections.Generic;

namespace SQLSkaner.IKeyWord
{
    class TableManipulation : IKeyWords
    {
        static readonly List<string> matchingRegex = new List<string>();

        public TableManipulation()
        {
            matchingRegex.Add("CREATE TABLE");
            matchingRegex.Add("ALTER TABLE");
            matchingRegex.Add("TRUNCATE TABLE");
            matchingRegex.Add("DROP TABLE");
        }

        public bool IsFullMatch(string input)
        {
            return MatchingService.IsFullMatch(matchingRegex, input);
        }

        public bool IsPartialMatch(string input)
        {
            return MatchingService.IsPartialMatch(matchingRegex, input);
        }

        public string KeyWordName()
        {
            return "TableManipulation";
        }
    }
}