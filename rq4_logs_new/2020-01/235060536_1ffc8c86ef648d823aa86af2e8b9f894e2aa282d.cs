using System.Collections.Generic;
using static System.Int32;

namespace TaxCalculator
{
    public static class StaticEmissions
    {
        public static Dictionary<int, int> PetrolEmissions = new Dictionary<int, int>
        {
            {0,0},
            {50, 10},
            {75, 25},
            {90, 105},
            {100, 125},
            {110, 145},
            {130, 165},
            {150, 205},
            {170, 515},
            {190, 830},
            {225, 1240},
            {255, 1760},
            {MaxValue, 2070}
        };
    }
}