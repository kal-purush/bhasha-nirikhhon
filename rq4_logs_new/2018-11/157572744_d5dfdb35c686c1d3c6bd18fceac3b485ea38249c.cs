using System;

namespace DSAAlgorythm.ExtensionMethods
{
    public static class ByteArrayExtension
    {
        public static byte[] Xor(this byte[] a, byte[] b)
        {
            if(a.Length != b.Length)
                throw new ArgumentException("Arrays should be the same length");

            byte[] xorResult = new byte[a.Length];

            for (int i = 0; i < xorResult.Length; i++)
            {
                xorResult[i] = (byte)(a[i] ^ b[i]);
            }

            return xorResult;
        }
    }
}