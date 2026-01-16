using System;

namespace TelCo.ColorCoder
{
    class ColorManual
    {
        public static void printManual()
        {
            int pairnumber = 1;
            for (int i = 0; i < 5; i++)
            {
                for (int j = 0; j < 5; j++)
                {
                    Console.WriteLine("Pair Number: {0}  Major{1}  Minor{2}\n", pairnumber, ColorMapper.colorMapMajor[i], ColorMapper.colorMapMinor[j]);
                    pairnumber++;
                }
            }
        }
    }
}