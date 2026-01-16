using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
#if DEBUG
using System.Diagnostics;
#endif

namespace Assignment1
{
    static class CrozzleValidation
    {
        private static Crozzle CrozzleToValidate;
        private static Wordlist WordlistToValidate;

        public static Crozzle Validate(Crozzle crozzle, Wordlist wordlist)
        {
            CrozzleToValidate = crozzle;
            WordlistToValidate = wordlist;

            CrozzleToValidate.Words = new Dictionary<string, WordLocation>();
            CrozzleToValidate.HorizontalWords = new List<string>();
            CrozzleToValidate.VerticalWords = new List<string>();

#if DEBUG
            Stopwatch sw = new Stopwatch();
            sw.Start();
#endif

            bool Valid = true;

            for (int y = 0; y < CrozzleToValidate.Height; y++)
            {
                for (int x = 0; x < CrozzleToValidate.Width; x++)
                {
                    if (isLetter(CrozzleToValidate[x, y]))    // Current Cell is letter
                    {
                        if (x + 1 < CrozzleToValidate.Width && isLetter(CrozzleToValidate[x + 1, y])) // Not far right column AND next cell is a Letter
                        {
                            if (x == 0 || isNotLetter(CrozzleToValidate[x - 1, y]))  // Is far Left column OR previous cell is not a Letter (Prevents checking end of completed words)
                            {
                                string wordIn = traverseHori(x, y);
                                if (wordIn != null)
                                {
                                    CrozzleToValidate.Words.Add(wordIn, new WordLocation(x, y));
                                    CrozzleToValidate.HorizontalWords.Add(wordIn);
                                }
                                else
                                {
                                    Valid = Valid & false;
                                }
                            }
                        }

                        if (y + 1 < CrozzleToValidate.Height && isLetter(CrozzleToValidate[x, y + 1]))   // Not bottom row AND cell below is a Letter
                        {
                            if (y == 0 || isNotLetter(CrozzleToValidate[x, y - 1])) // Is top row OR previous cell is not a Letter (Prevents checking end of completed words)
                            {
                                string wordIn = traverseVert(x, y);
                                if (wordIn != null)
                                {
                                    CrozzleToValidate.Words.Add(wordIn, new WordLocation(x, y));
                                    CrozzleToValidate.VerticalWords.Add(wordIn);
                                }
                                else
                                {
                                    Valid = Valid & false;
                                }
                            }
                        }
                    }
                }
            }

            Valid = Valid & checkIntersections();

            if (wordlist.Difficulty == "EXTREME") { Valid = Valid & contigiousCheck(); }

#if DEBUG
            sw.Stop();
            LogFile.WriteLine("\t[INFO] Validation Complete in {0}ms!", sw.ElapsedMilliseconds);
#endif
            if (Valid)
                return CrozzleToValidate;
            else
                return null;
        }

        public static int GetScore(Crozzle crozz, Wordlist wordl)
        {
            CrozzleToValidate = crozz;
            WordlistToValidate = wordl;
            int runningTotal = 0;

            if (WordlistToValidate.Difficulty != "EXTREME")
            {
                for (int y = 0; y < CrozzleToValidate.Height; y++)
                {
                    for (int x = 0; x < CrozzleToValidate.Width; x++)
                    {
                        char currentChar = CrozzleToValidate[x, y];
                        if (isLetter(currentChar))
                            runningTotal += getLetterScore(currentChar);
                    }
                }

                if (WordlistToValidate.Difficulty == "HARD")
                {
                    runningTotal += (CrozzleToValidate.Words.Count * 10);
                }
            }
            else
            {
                foreach (string wordH in CrozzleToValidate.HorizontalWords)
                {
                    CrozzleWord cwH = new CrozzleWord(wordH, CrozzleToValidate.Words[wordH]);
                    foreach (CrozzleWord cwV in CrozzleToValidate.IntersectedWords[wordH])
                    {
                        int foundX = -1;
                        for (int i = cwH.X; i < cwH.X + cwH.Length; i++)
                        {
                            if (i == cwV.X)
                            {
                                foundX = i;
                                break;
                            }
                        }

                        int foundY = -1;
                        for (int j = cwV.Y; j < cwV.Y + cwV.Length; j++)
                        {
                            if (j == cwH.Y)
                            {
                                foundY = j;
                                break;
                            }
                        }

                        if (foundX >= 0 && foundY >= 0)
                        {
                            runningTotal += getLetterScore(CrozzleToValidate[foundX, foundY]);
                        }
                    }
                }

                runningTotal += (CrozzleToValidate.Words.Count * 10);
            }

            return runningTotal;
        }

        private static int getLetterScore(char c)
        {
            int score = 0;
            switch (WordlistToValidate.Difficulty)
            {
                case "EASY":
                    score = 1;
                    break;
                case "MEDIUM":
                case "HARD":    // A = 1, B = 2, ... Z =26
                    score = (int)c - 64;
                    break;
                case "EXTREME":
                    switch (c)
                    {
                        case 'A':
                        case 'E':
                        case 'I':
                        case 'O':
                        case 'U':
                            score = 1;
                            break;
                        case 'B':
                        case 'C':
                        case 'D':
                        case 'F':
                        case 'G':
                            score = 2;
                            break;
                        case 'H':
                        case 'J':
                        case 'K':
                        case 'L':
                        case 'M':
                            score = 4;
                            break;
                        case 'N':
                        case 'P':
                        case 'Q':
                        case 'R':
                            score = 8;
                            break;
                        case 'S':
                        case 'T':
                        case 'V':
                            score = 16;
                            break;
                        case 'W':
                        case 'X':
                        case 'Y':
                            score = 32;
                            break;
                        case 'Z':
                            score = 64;
                            break;
                    }
                    break;
                default:    // Should never be reached as Difficulty check appears before score calculation;
                    score = -1;
                    break;
            }
            return score;
        }

        private static bool contigiousCheck()
        {
            List<string> Followed = new List<string>();
            List<string> ToFollow = new List<string>();

            //Get first word
            string currentWord = CrozzleToValidate.HorizontalWords[0];
            Followed.Add(currentWord);  // Add it to the list of processed words

            // Add every word it intersects with to a list of words to be processed
            foreach (CrozzleWord cw in CrozzleToValidate.IntersectedWords[currentWord])
            {
                if (Followed.Contains(cw.Word))
                    continue;
                else
                    ToFollow.Add(cw.Word);
            }

            // While there are still words to process
            while (ToFollow.Count > 0)
            {
                currentWord = ToFollow[0];// Get first word of the list
                // Add every word it intersects with to a list of words to be processed
                foreach (CrozzleWord cw in CrozzleToValidate.IntersectedWords[currentWord])
                {
                    // Unless the word is already on the list or has already been processed
                    if (ToFollow.Contains(cw.Word) || Followed.Contains(cw.Word))
                        continue;
                    else
                        ToFollow.Add(cw.Word);
                }
                // Add it to the list of processed words and remove from the 'to be processed' list
                Followed.Add(currentWord);
                ToFollow.Remove(currentWord);
            }

            bool Valid;
            // If the number of intersecting words in this chunk is equal to the 
            // total number of words on the crozzle there is only one chunk
            if (Followed.Count == CrozzleToValidate.Words.Count)
            {
                Valid = true;
            }
            else
            {
                LogFile.WriteLine("\t[!ERROR!] You can only have one group of connected words!");
                Valid = false;
            }
            
            return Valid;
        }

        #region Character Validation
        private static bool isLetter(char input)
        {
            Regex rgx = new Regex(@"[a-zA-Z]");
            return rgx.IsMatch(input.ToString());
        }

        private static bool isNotLetter(char input)
        {
            return !isLetter(input);
        }
        #endregion

        #region Word Traversal
        private static string traverseHori(int startX, int startY)
        {
            bool Valid = true;
            bool tempReturn = false;
            string currentWord = "";
            bool endOfWord = false;

            for (int currentX = startX; currentX < CrozzleToValidate.Width; currentX++)
            {
                char currentLetter = CrozzleToValidate[currentX, startY];
                if (isLetter(currentLetter))
                {
                    currentWord += currentLetter;
                    if (WordlistToValidate.Difficulty == "EASY")
                    {
                        tempReturn = wordProximityHori(currentX, startY);
                        Valid = Valid & tempReturn;
                    }
                }

                if ((currentX + 1) >= CrozzleToValidate.Width)
                {
                    endOfWord = true;
                }
                else
                {
                    char nextLetter = CrozzleToValidate[currentX + 1, startY];
                    if (isNotLetter(nextLetter))
                    {
                        endOfWord = true;
                    }
                }

                if (endOfWord)
                {
                    if (WordlistToValidate.Difficulty == "EASY")
                    {
                        tempReturn = wordProximityHoriFinal(currentX, startY);
                        Valid = Valid & tempReturn;
                    }

                    tempReturn = checkFullWord(currentWord);
                    Valid = Valid & tempReturn;
                    break;
                }
                else
                {
                    tempReturn = checkPartialWord(currentWord);
                    Valid = Valid & tempReturn;
                }
            }

            if (Valid)
                return currentWord;
            else
                return null;
        }

        private static string traverseVert(int startX, int startY)
        {
            bool Valid = true;
            bool tempReturn = false;

            string currentWord = "";
            bool endOfWord = false;

            for (int currentY = startY; currentY < CrozzleToValidate.Height; currentY++)
            {
                char currentLetter = CrozzleToValidate[startX, currentY];
                if (isLetter(currentLetter))
                {
                    currentWord += currentLetter;
                    if (WordlistToValidate.Difficulty == "EASY")
                    {
                        tempReturn = wordProximityVert(startX, currentY);
                        Valid = Valid & tempReturn;
                    }
                }

                if ((currentY + 1) >= CrozzleToValidate.Height)
                {
                    endOfWord = true;
                }
                else
                {
                    char nextLetter = CrozzleToValidate[startX, currentY + 1];
                    if (isNotLetter(nextLetter))
                    {
                        endOfWord = true;
                    }
                }

                if (endOfWord)
                {
                    if (WordlistToValidate.Difficulty == "EASY")
                    {
                        tempReturn = wordProximityVertFinal(startX, currentY);
                        Valid = Valid & tempReturn;
                    }

                    tempReturn = checkFullWord(currentWord);
                    Valid = Valid & tempReturn;
                    break;
                }
                else
                {
                    tempReturn = checkPartialWord(currentWord);
                    Valid = Valid & tempReturn;
                }
            }

            if (Valid)
                return currentWord;
            else
                return null;
        }
        #endregion

        #region Word Validation
        private static bool checkPartialWord(string word)
        {
            if (WordlistToValidate.StartsWith(word) == false)
            {
                LogFile.WriteLine("\t[WARN] No words in the Word list start with {0}", word);
                return false;
            }
            return true;
        }

        private static bool checkFullWord(string word)
        {
            if (WordlistToValidate.Contains(word) == false)
            {
                LogFile.WriteLine("\t[!ERROR!] Word not found! ({0})", word);
                return false;
            }
            else if (CrozzleToValidate.Words.ContainsKey(word))
            {
                LogFile.WriteLine("\t[!ERROR!] Duplicate word found! ({0})", word);
                return false;
            }
            else
            {
#if DEBUG
                LogFile.WriteLine("\t[INFO] New valid word found! ({0})", word);
#endif
                return true;
            }
        }
        #endregion

        #region Intersection Validation
        private static void getIntersections()
        {
            CrozzleToValidate.IntersectedWords = new Dictionary<string, List<CrozzleWord>>();

            foreach (string wordH in CrozzleToValidate.HorizontalWords)
            {
                foreach (string wordV in CrozzleToValidate.VerticalWords)
                {
                    if (CrozzleToValidate.Words[wordH].Y >= CrozzleToValidate.Words[wordV].Y
                        && CrozzleToValidate.Words[wordH].Y <= (CrozzleToValidate.Words[wordV].Y + wordV.Length - 1)
                        && CrozzleToValidate.Words[wordV].X >= CrozzleToValidate.Words[wordH].X
                        && CrozzleToValidate.Words[wordV].X <= (CrozzleToValidate.Words[wordH].X + wordH.Length - 1))
                    {
                        if (CrozzleToValidate.IntersectedWords.ContainsKey(wordH) == false)
                            CrozzleToValidate.IntersectedWords.Add(wordH, new List<CrozzleWord>());
                        CrozzleToValidate.IntersectedWords[wordH].Add(new CrozzleWord(wordV, CrozzleToValidate.Words[wordV]));

                        if (CrozzleToValidate.IntersectedWords.ContainsKey(wordV) == false)
                            CrozzleToValidate.IntersectedWords.Add(wordV, new List<CrozzleWord>());
                        CrozzleToValidate.IntersectedWords[wordV].Add(new CrozzleWord(wordH, CrozzleToValidate.Words[wordH]));
#if DEBUG
                        LogFile.WriteLine("\t[INFO] Intersection found! ({0}, {1})", wordH, wordV);
#endif
                    }
                }
            }
        }

        private static bool checkIntersections()
        {
            getIntersections();

            bool Valid = true;

            foreach (KeyValuePair<string, WordLocation> pair in CrozzleToValidate.Words)
            {
                if (CrozzleToValidate.IntersectedWords.ContainsKey(pair.Key))
                {
                    switch (WordlistToValidate.Difficulty)
                    {
                        case "EASY":
                        case "MEDIUM":
                            if (CrozzleToValidate.IntersectedWords[pair.Key].Count < 1 || CrozzleToValidate.IntersectedWords[pair.Key].Count > 2)
                            {
                                LogFile.WriteLine("\t[!ERROR!] {0} must intersect 1 or 2 other valid words", pair.Key);
                                Valid = Valid & false;
                            }
                            break;

                        case "HARD":
                        case "EXTREME":
                            if (CrozzleToValidate.IntersectedWords[pair.Key].Count < 1)
                            {
                                LogFile.WriteLine("\t[!ERROR!] {0} must intersect 1 or more other valid words", pair.Key);
                                Valid = Valid & false;
                            }
                            break;
                    }
                }
                else
                {
                    LogFile.WriteLine("\t[!ERROR!] {0} must intersect at least 1 other valid word", pair.Key);
                    Valid = Valid & false;
                }
            }
            return Valid;
        }
        #endregion

        #region Easy - Touching Words Detection - Horizontal
        private static bool wordProximityHori(int x, int y)
        {
            if (y > 0 && x > 0)    // Checks row above for touching words
            {
                if (isLetter(CrozzleToValidate[x, y - 1]) && isLetter(CrozzleToValidate[x - 1, y - 1]))
                {
                    LogFile.WriteLine("\t[!ERROR!] Touching word found at [{0}, {1}]", x, y);
                    //throw new CrozzleWordProximityException();
                    return false;
                }
            }

            if (y + 1 < CrozzleToValidate.Height && x > 0)    // Checks row below for touching words
            {
                if (isLetter(CrozzleToValidate[x, y + 1]) && isLetter(CrozzleToValidate[x - 1, y + 1]))
                {
                    LogFile.WriteLine("\t[!ERROR!] Touching word found at [{0}, {1}]", x, y);
                    //throw new CrozzleWordProximityException();
                    return false;
                }
            }

            return true;
        }

        private static bool wordProximityHoriFinal(int x, int y)
        {
            if (y > 0 && x + 1 < CrozzleToValidate.Width)    // Checks row above for touching words on last letter
            {
                if (isLetter(CrozzleToValidate[x, y - 1]) && isLetter(CrozzleToValidate[x + 1, y - 1]))
                {
                    LogFile.WriteLine("\t[!ERROR!] Touching word found at [{0}, {1}]", x, y);
                    //throw new CrozzleWordProximityException();
                    return false;
                }
            }

            if (y + 1 < CrozzleToValidate.Height && x + 1 < CrozzleToValidate.Width)    // Checks row below for touching words on last letter
            {
                if (isLetter(CrozzleToValidate[x, y + 1]) && isLetter(CrozzleToValidate[x + 1, y + 1]))
                {
                    LogFile.WriteLine("\t[!ERROR!] Touching word found at [{0}, {1}]", x, y);
                    //throw new CrozzleWordProximityException();
                    return false;
                }
            }

            return true;
        }
        #endregion

        #region Easy - Touching Words Detection - Vertical
        private static bool wordProximityVert(int x, int y)
        {
            if (x > 0 && y > 0)    // Checks left column for touching words
            {
                if (isLetter(CrozzleToValidate[x - 1, y]) && isLetter(CrozzleToValidate[x - 1, y - 1]))
                {
                    LogFile.WriteLine("\t[!ERROR!] Touching word found at [{0}, {1}]", x, y);
                    //throw new CrozzleWordProximityException();
                    return false;
                }
            }

            if (x + 1 < CrozzleToValidate.Width && y > 0)    // Checks right column for touching words
            {
                if (isLetter(CrozzleToValidate[x + 1, y]) && isLetter(CrozzleToValidate[x + 1, y - 1]))
                {
                    LogFile.WriteLine("\t[!ERROR!] Touching word found at [{0}, {1}]", x, y);
                    //throw new CrozzleWordProximityException();
                    return false;
                }
            }

            return true;
        }

        private static bool wordProximityVertFinal(int x, int y)
        {
            if (x > 0 && y + 1 < CrozzleToValidate.Height)    // Checks left column for touching words on last letter
            {
                if (isLetter(CrozzleToValidate[x - 1, y]) && isLetter(CrozzleToValidate[x - 1, y + 1]))
                {
                    LogFile.WriteLine("\t[!ERROR!] Touching word found at [{0}, {1}]", x, y);
                    //throw new CrozzleWordProximityException();
                }
            }

            if (x + 1 < CrozzleToValidate.Width && y + 1 < CrozzleToValidate.Height)    // Checks right column for touching words on last letter
            {
                if (isLetter(CrozzleToValidate[x + 1, y]) && isLetter(CrozzleToValidate[x + 1, y + 1]))
                {
                    LogFile.WriteLine("\t[!ERROR!] Touching word found at [{0}, {1}]", x, y);
                    //throw new CrozzleWordProximityException();
                }
            }

            return true;
        }
        #endregion
    }
}