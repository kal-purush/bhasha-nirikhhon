// Copyright (c) Arctium Emulation.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Framework.Cryptography
{
    public class Adler32
    {
        // This is not an optimized but a straightforward implementation. 
        public static uint Calculate(byte[] data, uint s1 = 1, uint s2 = 0)
        {
            for (var i = 0; i < data.Length; i++)
            {
                s1 = (s1 + data[i]) % 0xFFF1;
                s2 = (s2 + s1) % 0xFFF1;
            }

            return (s2 << 16) + s1;
        }
    }
}