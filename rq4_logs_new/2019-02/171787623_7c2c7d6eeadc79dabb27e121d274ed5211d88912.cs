using OpenNLP.Tools;
using OpenNLP.Tools.PosTagger;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web;


// If you need more models go to: https://github.com/AlexPoint/OpenNlp/tree/master/Resources/Models/Parser
namespace cpsc571.Helpers
{
    public class TweetParser
    {
        private const String MODEL_PATH = @"Helpers\Parser\Models\EnglishPOS.nbin";
        private const String DICT_PATH = @"Helpers\Parser\Models\Parser\tagdict";
        public void run()
        {
            const string testTweet = "I love my dog, she's so pretty! Anybody without a dog should really get one!";
            string currentDirectory = HttpRuntime.BinDirectory;
            string modelPath = Path.Combine(HttpRuntime.BinDirectory, MODEL_PATH);
            String tagDictDir = Path.Combine(HttpRuntime.BinDirectory, DICT_PATH);
            var posTagger = new EnglishMaximumEntropyPosTagger(modelPath, tagDictDir);
            string[] tokens = { "-", "Sorry", "Mrs.", "Hudson", ",", "I", "'ll", "skip", "the", "tea", "." };
            var pos = posTagger.Tag(tokens);
            Console.WriteLine(pos);
        }
    }
}