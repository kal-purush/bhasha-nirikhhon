namespace RuleEngineTester.RuleEngine
{
    public class CSVFileConditionParser : IConditionParser
    {
        string[] _data;
        string _separator;
        public CSVFileConditionParser(string fn, string separator)
        {
            _data = File.ReadAllLines(fn);
            _separator = separator;
        }

        public List<Condition> Parse()
        {
            var idx = 1;
            var result = new List<Condition>();
            foreach (var line in _data)
            {
                var tokens = line.Split(_separator);
                if (tokens.Length == 3)
                {
                    result.Add(new(idx, tokens[0], null, tokens[1], tokens[2]));

                }
            }


            return result;
        }

    }
}