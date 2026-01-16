namespace SQLSkaner.IKeyWord
{
    class LogicalOperators : IKeyWords
    {
        public bool IsFullMatch(string input)
        {
            input = input.ToUpper();
            if (input == "AND")
                return true;
            if (input == "OR")
                return true;
            if (input == "NOT")
                return true;

            return false;
        }

        public bool IsPartialMatch(string input)
        {
            input = input.ToUpper();
            if (PartialMatching.IsPartialMatchToRegex("AND", input))
                return true;
            if (PartialMatching.IsPartialMatchToRegex("OR", input))
                return true;
            if (PartialMatching.IsPartialMatchToRegex("NOT", input))
                return true;
            return false;
        }

        public string KeyWordName()
        {
            return "LogicalOperators";
        }
    }
}