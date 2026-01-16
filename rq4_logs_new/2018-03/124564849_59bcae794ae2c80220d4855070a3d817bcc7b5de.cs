using System.Collections.Generic;

namespace SQLSkaner
{
    class Skaner
    {
        List<FoundKeyWord> fullMatchList = new List<FoundKeyWord>();
        List<IKeyWords> _allPossibleKeywordsList = new List<IKeyWords>{new MathematicalOperators()};
        private string input;

        bool AnyPartilMatch(string currentInput)
        {
            foreach (var keyWord in _allPossibleKeywordsList)
            {
                if (keyWord.IsPartialMatch(currentInput)) return true;
            }
            return false;
        }

        List<FoundKeyWord> GetAllFullMatches(string currentInput)
        {
            var result = new List<FoundKeyWord>();
            foreach (var keyWord in _allPossibleKeywordsList)
            {
                if (keyWord.IsFullMatch(currentInput))
                {
                    result.Add(new FoundKeyWord(currentInput,keyWord));
                }
            }
            return result;
        }

        private List<FoundKeyWord> DoScan()
        {
            int startPosition = 0;
            int lengthOfCurrentWord = 1;
            var result = new List<FoundKeyWord>();

            while (startPosition + lengthOfCurrentWord < input.Length)
            {
                string inputSubstring = input.Substring(startPosition, lengthOfCurrentWord);
                var fullMatchSearchResult = GetAllFullMatches(inputSubstring);
                if(AnyPartilMatch())
            }

            return null;
        }
    }
}