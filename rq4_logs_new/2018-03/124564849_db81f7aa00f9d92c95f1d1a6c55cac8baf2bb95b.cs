using System.Collections.Generic;

namespace SQLSkaner.IKeyWord
{
    class Conditions : IKeyWords
    {
        static readonly List<string> matchingRegex = new List<string>();

        public Conditions()
        {
            matchingRegex.Add("WHERE");
            matchingRegex.Add("DISTINCT");
            matchingRegex.Add("ON");
            matchingRegex.Add("LIMIT");
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
            return "Conditions";
        }
    }
}