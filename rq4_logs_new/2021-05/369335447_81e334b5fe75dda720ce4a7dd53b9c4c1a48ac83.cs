using System;

namespace Acme.Common.Methods
{
    public class StringHandler
    {
        public string InsertSpaces(string source)
        {
            string result = string.Empty;
            if (!String.IsNullOrWhiteSpace(source))
            {
                foreach (char letter in source)
                {
                    if (char.IsUpper(letter))
                    {
                        result += " ";
                    }
                    result += letter;
                }
            }
            return result;
        }
    }
}