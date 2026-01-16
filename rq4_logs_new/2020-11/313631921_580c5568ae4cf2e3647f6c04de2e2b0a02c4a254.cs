using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace WindowsFormsApp1
{
    public class QuanLyTime
    {
        private int time1;
        public int Time1
        {
            get { return time1; }
            set { time1 = value; }
        }

        private int time2;
        public int Time2
        {
            get { return time2; }
            set { time2 = value; }
        }

        private int minute;
        public int Minute
        {
            get { return minute; }
            set { minute = value; }
        }

        private int sec;
        public int Sec
        {
            get { return sec; }
            set { sec = value; }
        }
        public QuanLyTime()
        {
            time1 = Constant.timePlayer1;
            time2 = Constant.timePlayer2;

            minute = 0;
            sec = 0;
        }
    }
}