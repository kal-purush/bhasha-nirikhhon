using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace MultiScreenWallpaper
{
    class NativeMethods
    {
        public const int HWND_BROADCAST = 0xffff;

        public static readonly int UPDATE = RegisterWindowMessage("UPDATE");
        public static readonly int DEBUG = RegisterWindowMessage("DEBUG");

        [DllImport("user32")]
        public static extern bool SendMessage(IntPtr hwnd, int msg, IntPtr wparam, IntPtr lparam);
        [DllImport("user32")]
        public static extern int RegisterWindowMessage(string message);
    }
}