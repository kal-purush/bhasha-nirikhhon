using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace WindowsFormsApp1.OtherClasses
{
    class PathHolderThread
    {
        MainPage main_page;
        private static System.Threading.Timer timer;
        private int TIME_INTERVAL = 3 * 1000;
        List<string> logs = new List<string>();
        List<FilePair<string, string, string>> file_logs = new List<FilePair<string, string, string>>();

        //Constructor
        public PathHolderThread(MainPage main_page)
        {
            this.main_page = main_page;
        }

        //Submethods
        public void Run()
        {
            timer = new System.Threading.Timer(Callback, null, TIME_INTERVAL, Timeout.Infinite);
        }

        public void Stop()
        {
            timer.Change(Timeout.Infinite, Timeout.Infinite); //stops the timer
            timer.Dispose();
            logs.Clear();
            file_logs.Clear();
        }


        private void Callback(Object state)
        {
            //Thread crash part

            string text = main_page.getMain_page_text_box().Text;
            
            // Long running operation
            timer.Change(TIME_INTERVAL, Timeout.Infinite);
        }
    }
}