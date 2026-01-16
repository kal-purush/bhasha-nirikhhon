using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace CH2.Helpers
{
    static class ExceptionExtensions
    {
        // Extension methods

        internal static Exception Log(this Exception ex)
        {
            File.AppendAllText("CaughtExceptions" + DateTime.Now.ToString("yyyy-MM-dd") + ".log", DateTime.Now.ToString("HH:mm:ss") + ": " + ex.Message + "\n" + ex.ToString() + "\n");
            return ex;
        }

        internal static Exception Display(this Exception ex, string msg = null, MessageBoxImage img = MessageBoxImage.Error)
        {
            MessageBox.Show(msg ?? ex.Message, "", MessageBoxButton.OK, img);
            return ex;
        }
    }
}