using System;
using System.Collections.Generic;
using UnityEngine;


namespace JevLogin
{
    public static class ColorManager
    {
        private static string[] _colorName =
        {
            "clear",
            "black",
            "blue",
            "cyan",
            "gray",
            "green",
            "magenta",
            "red",
            "white",
            "yellow"
        };

        private static Color[] _colorType =
        {
            Color.clear,
            Color.black,
            Color.blue,
            Color.cyan,
            Color.gray,
            Color.green,
            Color.magenta,
            Color.red,
            Color.white,
            Color.yellow
        };

        public static Dictionary<int, Color> ColorDictionary;
        public static Dictionary<Color, string> ColorName;

        static ColorManager()
        {
            ColorDictionary = new Dictionary<int, Color>();
            ColorName = new Dictionary<Color, string>();

            for (int i = 0; i < 10; i++)
            {
                if (!ColorDictionary.ContainsKey(i))
                {
                    ColorDictionary[i] = _colorType[i];
                    ColorName[ColorDictionary[i]] = _colorName[i];
                }
            }
        }

        public static Color GetValue(int a)
        {
            var color = ColorDictionary[a];
            return color;
        }
    }
}