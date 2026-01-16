namespace leetCodeSolutions
{
    public partial class Solution
    {
        /// <summary>
        /// https://leetcode.com/problems/maximum-number-of-words-you-can-type/
        /// <br/><b>1935. Maximum Number of Words You Can Type</b>
        /// <br/>There is a malfunctioning keyboard where some letter keys do not work. All other keys on the keyboard work properly.
        /// Given a string text of words separated by a single space(no leading or trailing spaces) and a string brokenLetters of all <b>distinct</b>
        /// letter keys that are broken, return the <b>number of words</b> in text you can fully type using this keyboard.
        /// </summary>
        /// <param name="text"></param>
        /// <param name="brokenLetters"></param>
        /// <returns></returns>
        public static int CanBeTypedWords(string text, string brokenLetters)
        {
            var arr = brokenLetters.ToCharArray();
            int wordCounter = 0;
            foreach (var word in text.Split(' '))
            {
                wordCounter++;
                foreach (var a in arr)
                    if (word.Contains(a))
                    {
                        wordCounter--;
                        break;
                    }
            }
            return wordCounter;
        }
    }
}