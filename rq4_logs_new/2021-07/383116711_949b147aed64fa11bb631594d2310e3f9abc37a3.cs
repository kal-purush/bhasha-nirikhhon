using AnagramSolver.Contracts.Interfaces;
using AnagramSolver.EF.CodeFirst.Models;
using AnagramSolver.Models.Models;
using System.Collections.Generic;

namespace AnagramSolver.BusinessLogic.Classes.CacheAnagrams
{
    public class CacheAnagramCodeFirst : ICacheAnagram
    {
        public CacheModel GetCachedAnagram(string command)
        {
            var cachedModel = new CacheModel();
            using (var context = new VocabularyCodeFirstContext())
            {
                var cachedAnagram = context.CachedWords.Find(command);
                if (cachedAnagram != null)
                {
                    cachedModel.Caches.Add(cachedAnagram.Anagram);
                    cachedModel.IsSuccessful = true;
                }
            }
            return cachedModel;
        }

        public void PutAnagramToCache(string command, List<string> listOfAnagrams)
        {
            var anagramToDB = string.Empty;
            foreach (var anagram in listOfAnagrams)
            {
                anagramToDB += $"{anagram}  ";
            }
            using (var context = new VocabularyCodeFirstContext())
            {
                var cachedWord = new CachedWord { Word = command, Anagram = anagramToDB };
                context.CachedWords.Add(cachedWord);
                context.SaveChanges();
            }
        }
    }
}