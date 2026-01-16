using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BookRecommender.DataManipulation
{
    class CsvParser
    {
        enum CharType { Comma, Quote, CR, LF, Other }
        string data;

        public CsvParser(string data)
        {
            this.data = data;
        }

        public List<List<string>> Parse(){
            var list = new List<List<string>>();
            foreach(var line in GetEnumerable()){
                list.Add(line);
            }
            return list;
        } 
        IEnumerable<List<string>> GetEnumerable()
        {
            var valuesBuffer = new List<string>();

            for (int position = 0; position < data.Length; position++)
            {
                switch (GetCharTypeOnPos(position))
                {
                    case CharType.Comma:
                        // If last was comma, then add empty to buffer
                        if(position == 0 || GetCharTypeOnPos(position - 1) == CharType.Comma){
                            valuesBuffer.Add(string.Empty);
                        }
                        continue;
                    case CharType.Quote:
                        // Find rest of the value, unescape, insert into buffer and skip
                        int posSecondSingleQuote = FindSecondSingleQuote(position);
                        string retNext = Unescape(GetSubstring(position + 1, posSecondSingleQuote - 1));
                        position = posSecondSingleQuote;
                        valuesBuffer.Add(retNext);
                        break;
                    case CharType.CR:
                        // Ignore by specification, there needs to be a LF on the end of line
                        // EOL = \r\n | \n
                        // So therefore I am waiting for \n
                        continue;
                    case CharType.LF:
                        //On end of line
                        yield return valuesBuffer;
                        valuesBuffer = new List<string>();
                        break;
                    case CharType.Other:
                        // Find rest of this value, insert into buffer and skip
                        int posEndOfValue = FindEndOfValue(position);
                        string value = GetSubstring(position, posEndOfValue);
                        position = posEndOfValue;
                        valuesBuffer.Add(value);
                        break;
                }
            }
            if(valuesBuffer.Count != 0){
                throw new InvalidDataException("CRLN on the end of file mising");
                // yield return valuesBuffer;
            }
            yield break;
        }

        CharType GetCharTypeOnPos(int position)
        {
            if (position >= data.Length)
            {
                throw new IndexOutOfRangeException();
            }
            if (data[position] == '"')
            {
                return CharType.Quote;
            }
            if (data[position] == ',')
            {
                return CharType.Comma;
            }
            if (data[position] == '\r')
            {
                return CharType.CR;
            }
            if (data[position] == '\n')
            {
                return CharType.LF;
            }
            return CharType.Other;

        }
        int FindSecondSingleQuote(int posOfFirstQuote)
        {
            for (int pos = posOfFirstQuote + 1; pos < data.Length; pos++)
            {
                bool isQuoteFirst = data[pos] == '"';

                bool onLastPosition = pos == data.Length;

                if (isQuoteFirst)
                {
                    if (onLastPosition)
                    {
                        return pos;
                    }

                    bool isQuoteSecond = isQuoteSecond = data[pos + 1] == '"';

                    if (isQuoteSecond)
                    {
                        // Double quote found - need to skip
                        // Increase pos by one to skip the next quote
                        pos++;
                    }
                    else
                    {
                        // Single quote found - done
                        return pos;
                    }
                }
                // If on pos is not a quote I dont care about the second pos - will be found later
            }
            return -1;
        }
        int FindEndOfValue(int posOfFirst)
        {
            // iterate while meeting data
            for (int pos = posOfFirst + 1; pos < data.Length; pos++)
            {
                if (GetCharTypeOnPos(pos) != CharType.Other)
                {
                    return pos - 1;
                }
            }
            // If forcycle was past, then we are on the end of data and all values were data
            return data.Length - 1;
        }


        string GetSubstring(int start, int end)
        {
            // 0 1 2 3 4 5 6 7 8 9 
            //     |         |

            var sizeSubstring = end - start + 1;
            return data.Substring(start, sizeSubstring);
        }
        string Unescape(string s)
        {
            return s.Replace("\"\"", "\"");
        }
    }
}