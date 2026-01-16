using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace AnagramSolver.BusinessLogic.Classes
{
    public class TextFileToHashSet
    {
        public void GetAllWords()
        {
            HashSet<string> readyList = new HashSet<string>();
            HashSet<string> items = new HashSet<string>(File.ReadLines(@"C:\Users\jonas.jaugelis\source\repos\AnagramSolver\zodynas.txt"));
            foreach (string item in items)
            {
                List<string> itemLine = item.Split("\t").ToList();
                foreach (string separateItem in itemLine)
                {
                    readyList.Add(separateItem);
                }
            }
        }
    }
}