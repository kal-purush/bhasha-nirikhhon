using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Data
{
    public static class WordManager
    {
        public static int VocabularyCount { get { return WordIdMap.Count; } }
        private static Dictionary<string, int> WordIdMap = new Dictionary<string,int>();
        private static Dictionary<int, string> IdWordMap = new Dictionary<int, string>();

        public static int ToWordId(this string word)
        {
            if (WordIdMap.ContainsKey(word))
                return WordIdMap[word];

            lock (WordIdMap)
            {
                if (!WordIdMap.ContainsKey(word))
                {
                    lock (WordIdMap)
                    {
                        var wordId = WordIdMap.Count;
                        WordIdMap.Add(word, wordId);
                        IdWordMap.Add(wordId, word);
                    }
                }
            }

            return WordIdMap[word];
        }

        public static string ToWord(this int wordId)
        {
            if (IdWordMap.ContainsKey(wordId))
                return IdWordMap[wordId];

            return null;
        }

        public static IEnumerable<KeyValuePair<string, int>> WordIterator()
        {
            foreach (var elem in WordIdMap)
                yield return elem;
        }

        public static void Clear()
        {
            lock (WordIdMap)
            {
                WordIdMap.Clear();
                IdWordMap.Clear();
            }
        }
    }

    public class Document
    {
        public readonly string Id;
        public readonly List<int> WordSequence = new List<int>();
        public int Count { get { return WordSequence.Count; } }

        private Document()
        {
        }

        public Document(string id, string content, params char[] delimiter)
        {
            Id = id;
            foreach (var word in content.ToLower().Split(delimiter))
            {
                if (word.Trim().Length == 0) continue;

                var wordId = word.ToWordId();
                WordSequence.Add(wordId);
            }
        }
    }

}