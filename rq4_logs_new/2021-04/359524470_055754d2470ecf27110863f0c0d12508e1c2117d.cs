using System.Collections.Generic;

namespace PhishingSites
{
    public class Engine
    {
        public List<string> PhishingDomainGenerateAlgorithm(string domain)
        {
            string[] sArray = domain.Split('.');
            int len = sArray.Length;
            string dLeftPart = string.Empty;
            string dRightPart = string.Empty;

            if (len == 1) return null;
            else if (len == 2)
            {
                dLeftPart = sArray[0];
                dRightPart = "." + sArray[1];
            }
            else if (len > 2)
            {
                int count = sArray[len - 2].Length;
                if (count < 4) dRightPart = "." + sArray[len - 2] + "." + sArray[len - 1];
                else dRightPart = "." + sArray[len - 1];
                dLeftPart = domain.Replace(dRightPart, "");
            }

            List<List<char>> cList2 = new List<List<char>>();
            bool isFound = false;
            char[] cArray = dLeftPart.ToCharArray();

            foreach (char c in cArray)
            {
                if (dict.ContainsKey(c))
                {
                    isFound = true;
                    cList2.Add(dict[c]);
                }
                else cList2.Add(new List<char>() { c });
            }

            if (!isFound) return null;
            else
            {
                List<string> tempList = new List<string>();
                int count = cList2.Count;

                for (int i = 0; i < count; i++)
                {
                    int t = tempList.Count;

                    foreach (char c in cList2[i])
                    {
                        for (int j = 0; j < t; j++)
                        {
                            tempList.Add(tempList[j] + c.ToString());
                        }

                        if (i == 0) tempList.Add(c.ToString());
                    }

                    while (t > 0)
                    {
                        tempList.RemoveAt(t - 1);
                        t--;
                    }
                }

                List<string> fakeDomainList = new List<string>();
                foreach (string s in tempList) fakeDomainList.Add(s + dRightPart);
                return fakeDomainList;
            }
        }

        private readonly Dictionary<char, List<char>> dict = new Dictionary<char, List<char>>()
        {
            { '1', new List<char>() { '1', 'l', 'I' } },
            { 'l', new List<char>() { 'l', '1' } },

            { '2', new List<char>() { '2', 'Z' } },
            { 'z', new List<char>() { 'z', '2' } },

            { '4', new List<char>() { '4', 'f' } },
            { 'f', new List<char>() { 'f', '4' } },

            { '5', new List<char>() { '5', 'S' } },
            { 's', new List<char>() { 's', '5' } },

            { '6', new List<char>() { '6', 'b', 'G' } },
            { 'b', new List<char>() { 'b', '6' } },

            { '7', new List<char>() { '7', 'T' } },

            { '8', new List<char>() { '8', 'B', 'R' } },

            { '9', new List<char>() { '9', 'g' } },
            { 'g', new List<char>() { 'g', '9' } },

            { '0', new List<char>() { '0', 'D', 'O', 'Q' } },
            { 'o', new List<char>() { 'o', '0', 'Q' } }

            //...Custom...
        };
    }
}