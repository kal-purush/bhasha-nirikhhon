using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

/* Lokaverkefni
 * Vor 2017
 * Erla Óskarsdóttir
 * Hrafnkell Þorri Þrastarson */

namespace Lokaverkefni_Klasasafn
{
    public class FastThinking
    {
        //instance varibles skilgreindar
        private int score;
        private int totalTime;

        //Smiðurinn tekur inn 2 breytur
        public FastThinking(int s, int time)
        {
            score = s;
            totalTime = time;
        } //End á smið

        //Public Property sem skilar frá sér instance breytu
        public int Score
        {
            get
            {
                return score;
            } //End á get
        } //End á Score

        //Public Property sem skilar frá sér instance breytu
        public int TotalTime
        {
            get
            {
                return totalTime;
            } //End á get
        } //End á TotalTime
    }
}