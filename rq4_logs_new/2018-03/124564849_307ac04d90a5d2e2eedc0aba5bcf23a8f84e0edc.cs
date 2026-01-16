namespace SQLSkaner
{
    class Agregators : IKeyWords
    {
        public bool IsPartialMatch(string input)
        {
            switch (input.ToUpper())
            {
                case "A":
                    return true;
                case "AV":
                    return true;
                case "S":
                    return true;
                case "SU":
                    return true;
                default:
                    return false;
            }
        }

        public bool IsFullMatch(string input)
        {
            switch (input.ToUpper())
            {
                case "AVG":
                    return true;
                case "SUM":
                    return true;
            }

            return false;
        }

        public string KeyWordName()
        {
            return "Agregators";
        }
    }
}