using System;
using System.Collections.Generic;

namespace Dictionaries
{
    class Program
    {
        enum Color { Red, Green, Blue, Purple, Black };
        static void Main(string[] args)
        {
            // Dictionaries har en egen initializer-syntax
            Dictionary<Color, string> colorNames = 
                new Dictionary<Color, string>{
                { Color.Red, "Rött" },
                { Color.Green,  "Grönt"}, 
                { Color.Blue, "Blått"}
            };
            // Ett dictionary kan indexieras med sin nyckeltyp.
            // Kastar exception om nyckeln inte har något värde.
            Console.WriteLine(colorNames[Color.Red]);
            // Metoden 'GetValueOrDefault' för att riskera exception.
            colorNames.GetValueOrDefault(Color.Purple, "Okänd");
            if (!colorNames.ContainsKey(Color.Purple))
            {
                colorNames[Color.Purple] = "Lila";
            }
            


        }
    }
}