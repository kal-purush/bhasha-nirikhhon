using AnagramSolver.Contracts.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;

namespace AnagramSolver.BusinessLogic.Classes
{
    public class AnagramSolver : IAnagramSolver
    {
        WordRepository word = new WordRepository();
        public List<string> GetAnagrams(string command)
        {
            var allWords = word.GetAllWords();

            List<string> sameLengthWordToCharArray = new List<string>();
            List<string> newList = new List<string>();

            char[] commandChars = command.ToCharArray();
            Array.Sort(commandChars);

            foreach (var word in allWords)
            {
                if(word.Length == command.Length)
                {
                    sameLengthWordToCharArray.Add(word);
                }
            }
            foreach(var word in sameLengthWordToCharArray)
            {
                char[] wordChars = word.ToCharArray();
                Array.Sort(wordChars);
                for (int i = 0; i < command.Length; i++)
                {
                    bool isEqual = Enumerable.SequenceEqual(wordChars, commandChars);
                    if (isEqual)
                    {
                        newList.Add(word);
                    }
 
                }
                
            }
            return newList;
        }
    }
}