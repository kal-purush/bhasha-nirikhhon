using System;
using System.Collections.Generic;
using System.Text;
using System.Drawing;

namespace TelCo.ColorCoder
// This class makes a pair of color for major and minor color.
{   class ColorPair
    {   private Color majorColor;
        public Color MajorColor
        { get { return majorColor; } set { majorColor = value; } }
        private Color minorColor;
        public Color MinorColor
        { get { return this.minorColor; } set { this.minorColor = value; } }
        public override string ToString(){
            Dictionary<string, int> whiteMap = new Dictionary<string, int>();
            //for {minor color , painumber}
            if (majorColor == Color.White){
                whiteMap.Add("Blue",1);whiteMap.Add("Orange", 2);
                whiteMap.Add("Green", 3);
                whiteMap.Add("Brown", 4);whiteMap.Add("SlateGray", 5);}
            if (majorColor == Color.Red){
                whiteMap.Add("Blue", 6);whiteMap.Add("Orange", 7);
                whiteMap.Add("Green", 8);whiteMap.Add("Brown", 9);
                whiteMap.Add("SlateGray", 10);}
            if (majorColor == Color.Black){
                whiteMap.Add("Blue", 11);whiteMap.Add("Orange", 12);
                whiteMap.Add("Green", 13);whiteMap.Add("Brown", 14);
                whiteMap.Add("SlateGray", 15);}
            if (majorColor == Color.Yellow){
                whiteMap.Add("Blue", 16);whiteMap.Add("Orange", 17);
                whiteMap.Add("Green", 18);whiteMap.Add("Brown", 19);
                whiteMap.Add("SlateGray", 20);}
            if (majorColor == Color.Violet){
                whiteMap.Add("Blue", 21);whiteMap.Add("Orange", 22);
                whiteMap.Add("Green", 23);whiteMap.Add("Brown", 24);
                whiteMap.Add("SlateGray", 25);}
            return string.Format("MajorColor:{0}, MinorColor:{1}", majorColor.Name, minorColor.Name);
            //return string.Format("MajorColor:{0}, MinorColor:{1}, PairNumber:{3}", majorColor.Name, minorColor.Name,pairNumber);
        }}}