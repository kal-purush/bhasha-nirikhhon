using AnagramSolver.Contracts.Interfaces;
using AnagramSolver.EF.DatabaseFirst.Models;
using AnagramSolver.Models.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AnagramSolver.BusinessLogic.Classes
{
    public class CacheAnagramWithEF : ICacheAnagram
    {
        public CacheModel GetCachedAnagram(string command)
        {
            var listOfAnagramsToReturn = new List<string>();
            var cachedModel = new CacheModel();
            using (var context = new VocabularyDBContext())
            {                
                var listOfAnagrams = context.CachedWords
                    .Where(c => c.Word.Equals(command))
                    .ToList();
                foreach(var element in listOfAnagrams)
                {
                    var anagramsToReturn = element.Anagram;
                    listOfAnagramsToReturn.Add(anagramsToReturn);
                    cachedModel.Caches = listOfAnagramsToReturn;
                    cachedModel.IsSuccessful = true;
                }
            }
            return cachedModel;
        }

        public void PutAnagramToCache(string command, List<string> listOfAnagrams)
        {
            var anagramToDB = "";
            foreach (var anagram in listOfAnagrams)
            {
                anagramToDB += $"{anagram}  ";
            }
            using (var context = new VocabularyDBContext())
            {
                var cachedWord = new CachedWord { Word = command, Anagram = anagramToDB };
                context.CachedWords.Add(cachedWord);
                context.SaveChanges();
            }
        }
    }
}