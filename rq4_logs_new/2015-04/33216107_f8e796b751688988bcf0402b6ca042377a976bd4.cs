using System;
using System.Drawing;
using System.Windows.Forms;

namespace Pomo
{
    static class Program
    {
        enum State
        {
            Work,
            Rest
        }

        #region Private Members

        readonly static TimeSpan WorkPeriod = new TimeSpan(0, 25, 0);
        readonly static TimeSpan RestPeriod = new TimeSpan(0, 5, 0);

        static NotifyIcon _notifyIcon;
        static Timer _timer;
        static TimeSpan _currentInterval = WorkPeriod;
        static State _currentState;

        #endregion

        #region Entry Point

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);

            AddTrayIcon();
            SetupTimer();

            Application.Run();

            RemoveTrayIcon();
        }

        #endregion

        #region Private Methods

        static void SetupTimer()
        {
            _timer = new Timer()
            {
                Interval = 1000,
                Enabled = true
            };

            _timer.Tick += Timer_Tick;
        }

        static void StartTimer()
        {
            _timer.Start();
        }

        static void PauseTimer()
        {
            _timer.Stop();
        }

        static void ResetTimer()
        {
            _currentState = State.Work;
            _currentInterval = WorkPeriod;
        }

        static void AddTrayIcon()
        {
            var menu = new ContextMenu();

            var exitItem = new MenuItem { Text = "E&xit" };
            exitItem.Click += (sender, e) => { Application.Exit(); };
            var startItem = new MenuItem { Text = "&Start" };
            startItem.Click += (sender, e) => { StartTimer(); };
            var stopItem = new MenuItem { Text = "&Pause" };
            stopItem.Click += (sender, e) => { PauseTimer(); };
            var resetItem = new MenuItem { Text = "&Reset" };
            resetItem.Click += (sender, e) => { ResetTimer(); };

            menu.MenuItems.Add(resetItem);
            menu.MenuItems.Add(new MenuItem("-"));
            menu.MenuItems.Add(startItem);
            menu.MenuItems.Add(stopItem);
            menu.MenuItems.Add(new MenuItem("-"));
            menu.MenuItems.Add(exitItem);

            _notifyIcon = new NotifyIcon
            {
                Visible = true,
                ContextMenu = menu,
            };

            _notifyIcon.Click += Icon_Click;

            UpdateIcon();
        }

        static void RemoveTrayIcon()
        {
            _notifyIcon.Visible = false;
        }

        static Icon GetIcon(string text = "0")
        {
            var backgroundColor = _currentState == State.Work ? Color.FromArgb(0xa1, 0x1c, 0x1c) : Color.FromArgb(0x15, 0x4f, 0x12);

            using (var backgroundBrush = new SolidBrush(backgroundColor))
            using (var borderPen = new Pen(Color.White))
            using (var textBrush = new SolidBrush(Color.White))
            {
                using (var bitmap = new Bitmap(16, 16))
                {
                    using (var graphics = Graphics.FromImage(bitmap))
                    {
                        graphics.FillRectangle(backgroundBrush, new Rectangle(0, 0, 16, 16));
                        graphics.DrawRectangle(borderPen, new Rectangle(0, 0, 15, 15));
                        graphics.DrawString(text, SystemFonts.DefaultFont, textBrush, 0, 1);

                        return Icon.FromHandle(bitmap.GetHicon());
                    }
                }
            }
        }

        static void UpdateIcon()
        {
            var text = _currentInterval.Seconds.ToString();
            if (_currentInterval.TotalSeconds > 60)
            {
                text = _currentInterval.Minutes.ToString();
            }

            _notifyIcon.Icon = GetIcon(text);
        }

        #region Event Handlers

        static void Icon_Click(object sender, EventArgs e)
        {
        }

        static void Timer_Tick(object sender, EventArgs e)
        {
            _currentInterval = _currentInterval.Add(new TimeSpan(0, 0, -1));

            if (_currentInterval.TotalSeconds == 0)
            {
                switch (_currentState)
                {
                    case State.Work:
                        _currentState = State.Rest;
                        _currentInterval = RestPeriod;
                        break;

                    case State.Rest:
                        _currentState = State.Work;
                        _currentInterval = WorkPeriod;
                        break;
                }
            }

            UpdateIcon();
        }

        #endregion

        #endregion
    }
}