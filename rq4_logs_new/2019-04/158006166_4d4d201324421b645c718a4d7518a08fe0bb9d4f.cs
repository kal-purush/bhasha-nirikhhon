
using System;

namespace LeedCodeLib.Algos.easy
{
    // 771
    public class JewelsAndStones
    {
        
        public int NumJewelsInStones(string J, string S)
        {
            if (string.IsNullOrEmpty(J)) throw new ArgumentException("message", nameof(J));
            if (string.IsNullOrEmpty(S)) throw new ArgumentException("message", nameof(S));

            int Output = 0;

            char[] inputchars1 = J.ToCharArray();
            int JCount = inputchars1.Length;

            char[] inputchars2 = S.ToCharArray();
            int SCount = inputchars2.Length;

            for (int SCountIndex = 0; SCountIndex < SCount; SCountIndex++)
            {
                for (int JCountIndex = 0; JCountIndex < JCount; JCountIndex++)
                {
                    if (inputchars1[JCountIndex] == inputchars2[SCountIndex])
                    {
                        Output++;
                    }
                }
            }
            return Output;
        }
    }
}