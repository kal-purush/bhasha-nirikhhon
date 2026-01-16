using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace WG_ConfigDifficulty
{
    // Demand curves are linear and sigmoid
    // Costs include, linear, log
    public enum MODIFIER
    {
        off,  // This just returns the original value
        linear,
        logarithmic,
        percentage,
        sigmoid
    }

    struct WG_MathParam
    {
        public WG_MathParam(double mul, double off)
        {
            multiplier = mul;
            offset = off;
        }

        public double multiplier;
        public double offset;
    }

    class DataStore
    {
        public static int CONSTRUCT = 0;
        public static int MAINT = 1;
        public static int RELOC = 2;
        public static int REFUND = 3;
        public static int DEMAND_RES = 4;
        public static int DEMAND_COM = 5;
        public static int DEMAND_IND = 6;
        public static WGCD_Math[] calcObjects = new WGCD_Math[7];

        public static int LOW_DENSITY = 0;
        public static int HIGH_DENSITY = 1;

        public static int[][] residentialLandValue = { new int[] { 0, 0, 0, 0, 0 },
                                                       new int[] { 0, 0, 0, 0, 0 } };
        public static int[][] residentialLandValue_Low = { new int[] { 0, 0, 0, 0, 0 },
                                                           new int[] { 0, 0, 0, 0, 0 } };

        public static int[][] commercialLandValue = { new int[] { 0, 0, 0, 0, 0 },
                                                      new int[] { 0, 0, 0, 0, 0 } };
        public static int[][] commercialLandValue_Low = { new int[] { 0, 0, 0, 0, 0 },
                                                          new int[] { 0, 0, 0, 0, 0 } };
    }
}
