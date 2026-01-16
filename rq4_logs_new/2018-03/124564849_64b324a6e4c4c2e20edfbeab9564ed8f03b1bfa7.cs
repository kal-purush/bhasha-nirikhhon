using System.Collections.Generic;

namespace SQLSkaner.IKeyWord
{
    class RecordManipulation : IKeyWords
    {
        static readonly List<string> matchingRegex = new List<string>();

        public RecordManipulation()
        {
            matchingRegex.Add("SELECT");
            matchingRegex.Add("INSERT INTO");
            matchingRegex.Add("UPDATE");
            matchingRegex.Add("DELETE FROM");
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
            return "RecordManipulation";
        }
    }
}