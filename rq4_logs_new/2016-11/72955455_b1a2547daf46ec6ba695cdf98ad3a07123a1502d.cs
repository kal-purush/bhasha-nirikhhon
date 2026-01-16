using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Text;
using System.Runtime.InteropServices;
using System.Windows.Forms;

namespace RestoreMonitor
{
    class MonitorState
    {
        public static List<WindowInfo> Windows { get; private set; }

        #region System Imports

        protected delegate bool EnumWindowsProc(IntPtr hWnd, IntPtr lParam);
        [DllImport("user32.dll", CharSet = CharSet.Unicode)]
        protected static extern int GetWindowText(IntPtr hWnd, StringBuilder strText, int maxCount);
        [DllImport("user32.dll", CharSet = CharSet.Unicode)]
        protected static extern int GetWindowTextLength(IntPtr hWnd);
        [DllImport("user32.dll")]
        protected static extern bool EnumWindows(EnumWindowsProc enumProc, IntPtr lParam);
        [DllImport("user32.dll")]
        protected static extern bool IsWindowVisible(IntPtr hWnd);
        [DllImport("user32.dll", EntryPoint = "SetWindowPos")]
        public static extern IntPtr SetWindowPos(IntPtr hWnd, int hWndInsertAfter, int x, int Y, int cx, int cy, int wFlags);
        [DllImport("user32.dll", SetLastError = true)]
        static extern bool GetWindowRect(IntPtr hwnd, ref RECT lpRect);

        #endregion


        /// <summary>
        /// Save the state of current windows
        /// </summary>
        /// <returns></returns>
        public static List<WindowInfo> SaveMonitorState(out string log)
        {
            // Empty log
            var logTemp = string.Empty;

            // Queue of top level windows
            Windows = new List<WindowInfo>();

            // Enum the windows
            EnumWindows(delegate (IntPtr hWnd, IntPtr param)
            {
                int size = GetWindowTextLength(hWnd);
                if (size++ > 0 && IsWindowVisible(hWnd))
                {
                    StringBuilder sb = new StringBuilder(size);
                    GetWindowText(hWnd, sb, size);
                    // Create a window window
                    var window = new WindowInfo
                    {
                        Handle = hWnd,
                        Rect = new Rectangle(),
                        Caption = sb.ToString(),
                        Level = 1
                    };


                    // Get the Window rectangle
                    SetWindowRectangle(window);

                    // Add to list
                    Windows.Add(window);

                    // Log added window
                    logTemp += "\n" + window;
                }

                // but return true here so that we iterate all windows
                return true;
            }, IntPtr.Zero);

            log = logTemp;
            return Windows;
        }


        public static void RestoreMointorState(out string log)
        {
            // Clear log
            log = string.Empty;

            // If there is no windows saved then return
            if (Windows == null) return;
            if (Windows.Count == 0) return;

            // Loop over the windows and position them
            foreach (var window in Windows)
            {
                // Get the current location of the window
                var location = GetWindowRectangle(window.Handle);

                // Location did not change then next
                if (location == window.Rect)
                {
                    log += "\nSKIPPED: " + window.Caption;
                    continue;
                }

                // Move the window to the stored location
                log += "\nMOVED: " + window.Caption;
                Move(window);

                // Update the stored location
                SetWindowRectangle(window);
            }

        }


        #region Simple Helper Methods
        public static Rectangle SetWindowRectangle(WindowInfo window)
        {
            window.Rect = GetWindowRectangle(window.Handle);
            return window.Rect;
        }

        public static Rectangle GetWindowRectangle(IntPtr hWnd)
        {
            var rect = new RECT();
            Rectangle rectangle = new Rectangle();
            if (!GetWindowRect(hWnd, ref rect)) return rectangle;

            rectangle.X = rect.Left;
            rectangle.Y = rect.Top;
            rectangle.Width = rect.Right - rect.Left + 1;
            rectangle.Height = rect.Bottom - rect.Top + 1;

            return rectangle;
        }

        public static void Move(WindowInfo window)
        {
            const short SWP_NOZORDER = 0X4;
            const int SWP_SHOWWINDOW = 0x0040;

            // Only valid windows with a location set
            if (window?.Handle == IntPtr.Zero) return;

            SetWindowPos(window.Handle, 0, window.Rect.X, window.Rect.Y,
                window.Rect.Width, window.Rect.Height, SWP_NOZORDER | SWP_SHOWWINDOW);
        }

        #endregion
    }



    internal class WindowInfo
    {
        public IntPtr Handle;
        public Rectangle Rect;
        public int Level;
        public string Caption;

        public override string ToString()
        {
            return $"{Caption} => [{Rect}]";
        }

    }



    [StructLayout(LayoutKind.Sequential)]
    public struct RECT
    {
        public int Left;        // x position of upper-left corner
        public int Top;         // y position of upper-left corner
        public int Right;       // x position of lower-right corner
        public int Bottom;      // y position of lower-right corner

        public override string ToString() => $"{{ x={Left}, y={Top}, Right={Right}, Bottom={Bottom} }}";
    }

}
