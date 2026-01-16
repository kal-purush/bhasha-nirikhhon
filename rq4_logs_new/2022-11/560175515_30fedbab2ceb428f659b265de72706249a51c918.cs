using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace IELTSpeaking.Helpers
{
    static class Controller
    {
        public static void StartTimer(Timer timer)
        {
            timer.Start();
        }
        public static void StopTimer(Timer timer)
        {
            timer.Stop();
        }
        public static void WriteControl(Control control, string str)
        {
            control.Text = str;
        }
        public static void HideControl(Control control)
        {
            control.Visible = false;
        }
        public static void Disable(Control control)
        {
            control.Enabled = false;
        }
        public static void ShowControl(Control control)
        {
            control.Visible = true;
        }
        public static void Enable(Control control)
        {
            control.Enabled = true;
        }
    }
}