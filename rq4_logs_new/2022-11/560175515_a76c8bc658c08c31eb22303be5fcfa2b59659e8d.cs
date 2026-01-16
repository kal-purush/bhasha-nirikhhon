using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using IELTSpeaking.Helpers;

namespace IELTSpeaking
{
    class Game
    {
        public static string text = "Good afternoon. My name is Kristina Pollock. Could I have your name, please?";
        public static bool isOn = true;
        private static int _stage = 0;
        private static List<string> _questionsPart1 = new List<string>{ "hi" };

        public static void Start()
        {
            ReadDatabase readDatabase = new ReadDatabase();
            //questionsPart1 = readDatabase.ReadPart1();
        }
        public static void NextStage()
        {
            bool isEnd = _stage == (_questionsPart1.Count);
            if (isEnd)
            {
                isOn = false;
                return;
            }

            text = _questionsPart1[_stage];
            _stage++;
        }
    }
}