using System;
using System.Collections.Generic;
using System.Text;

namespace TelCo.ColorCoder
{
    // This class is responsible to map the corresponding
    //color from the given pairnumber
    class ColorMapper : MyColor
    {
        // Given a pair number function returns the major and minor colors in that order
        // <param name="pairNumber">Pair number of the color to be fetched</param>
        // Super Class Color so that we can use the array of minot and major color.
        public static ColorPair GetColorFromPairNumber(int pairNumber)
        {
            // The function supports only 1 based index. Pair numbers valid are from 1 to 25
            int minorSize = colorMapMinor.Length;
            int majorSize = colorMapMajor.Length;
            if (pairNumber < 1 || pairNumber > minorSize * majorSize)
            {
                throw new ArgumentOutOfRangeException(
                    string.Format("Argument PairNumber:{0} is outside the allowed range", pairNumber));
            }

            // Find index of major and minor color from pair number
            int zeroBasedPairNumber = pairNumber - 1;
            int majorIndex = zeroBasedPairNumber / minorSize;
            int minorIndex = zeroBasedPairNumber % minorSize;

            //return val from the arrays
            ColorPair pair = new ColorPair();
            pair.MajorColor = colorMapMajor[majorIndex];
            pair.MinorColor = colorMapMinor[minorIndex];

            // return the value
            return pair;
        }
    }
}