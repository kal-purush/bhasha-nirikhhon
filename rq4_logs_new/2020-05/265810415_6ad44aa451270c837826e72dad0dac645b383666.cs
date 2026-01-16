using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace grayzen.State
{
    class TimedColorSession
    {
        private static System.Windows.Forms.Timer timer;

        public static void StartTimedColourSession(int milliseconds)
        {
            if(timer != null ) {
                timer.Dispose();
            }
            timer = new System.Windows.Forms.Timer();
            timer.Interval = milliseconds;
            timer.Tick += new EventHandler(BackToGrayscaleWhenTimedModeEnds);
            timer.Start();
            AppState.GoColour();
        }

        public static void ToggleTimedColourSession(int milliseconds)
        {
            if (IsRunning())
            {

            }
        }

        public static bool IsRunning()
        {
            return 
                timer != null && 
                timer.Enabled;
        }

        public static void EndTimerEarly()
        {
            if(timer == null) { return; }
            timer.Dispose();
            AppState.GoGrey();
        }

        private static void BackToGrayscaleWhenTimedModeEnds(Object source, EventArgs e)
        {
            if (source is System.Windows.Forms.Timer timer)
            {
                timer.Dispose();
            }
            AppState.GoGrey();
        }

    }
}