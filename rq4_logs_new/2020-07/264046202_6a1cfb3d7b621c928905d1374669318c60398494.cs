using ComboCounter.Classes;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Timers;
using System.Windows.Forms;

namespace ComboCounter.CustomControls
{

    // This class will show a visual representation of the current hit
    // The low medium and high are the colors which are used for the bar
    // The hits are input with the PushPunch method
    // When using this class it is important that before the window is destroyed cleanup is called
    // If cleanup is not called an exception may occur since the timers would stil continue going
    // Adding the control to the parent control should be the last thing when creating the control
    // If the control is added by the designed the adding is automatically after the creation
    class VisualFeedbackControl : UserControl
    {
        // Holds onto punches to queue up for animation
        // Most likely will only hold onto 1 punch at a time
        // Just in case the punches are coming faster than we can animate them
        private Queue<float> punchQueue = new Queue<float>(5);
        const float MAX_FORCE = 400;
        const int MIN_LENGTH = 40;
        const int MAX_LENGTH = 500;
        TimeSpan animationDuration = new TimeSpan(1000000);

        Color Low;
        Color Medium;
        Color High;

        // Essentially acts like a while loop checking for new punches to animate
        // Also has the duty of animating the retracting animation when not animating
        System.Timers.Timer InitializerTimer;

        // Animates the increasing bar when a new hit is input into the system
        System.Timers.Timer AnimationTimer;

        bool isRunning = false;

        // A reference point for the animationtimer to know how long it has been animating for
        DateTime animationStart;

        float current_Force = 0.0f;

        // A reference point to know how far the bar has to go to get to the next value
        int lastWidth = 0;
        int currWidth = 0;

        // Calculated at the end of an animation
        // Is calculated to be approx. 5 seconds to fully retract with a minimum of 1 per tick
        int decreasePerTick = 2;

        UserControl bar;

        List<UserControl> hashMarks;

        public VisualFeedbackControl()
            : this(Color.Red, Color.Yellow, Color.Green) {}

        public VisualFeedbackControl(Color low, Color medium, Color high)
        {
            Low = low;
            Medium = medium;
            High = high;

            punchQueue = new Queue<float>();

            bar = new UserControl();

            InitializerTimer = new System.Timers.Timer();
            InitializerTimer.Interval = 10;
            InitializerTimer.Elapsed += InitializerTimer_Tick;
            InitializerTimer.Start();
        }

        public void FinishSetup()
        {
            Width = MAX_LENGTH;
            BackColor = Color.Transparent;

            bar.Height = (int)(Height * 0.8);
            bar.Left = 0;
            bar.Anchor = AnchorStyles.Left | AnchorStyles.Top;
            bar.Width = 0;
            bar.Top = (Height - bar.Height) / 2;

            Controls.Add(bar);

            int numMarks = MAX_LENGTH / 40;

            hashMarks = new List<UserControl>(numMarks);
            for (int i = 0; i < numMarks; i++)
            {
                hashMarks.Add(new UserControl());
                hashMarks[i].Width = 5;
                hashMarks[i].BackColor = ThemeManager.initTextColor();
                hashMarks[i].Height = Height;
                hashMarks[i].Anchor = AnchorStyles.Left;
                hashMarks[i].Left = 40 * i;
                Controls.Add(hashMarks[i]);
            }
        }

        protected override void OnParentChanged(EventArgs e)
        {
            base.OnParentChanged(e);
            FinishSetup();
        }

        private void InitializerTimer_Tick(object sender, EventArgs e)
        {
            bar.Height = (int)(Height * 0.8);

            if (!isRunning && punchQueue.Count > 0)
            {
                current_Force = punchQueue.Dequeue();
                AnimationTimer = new System.Timers.Timer();
                AnimationTimer.Interval = 10;
                AnimationTimer.Elapsed += AnimationTimer_Tick;
                currWidth = (int)Math.Min(MAX_LENGTH, MAX_LENGTH * (current_Force / MAX_FORCE) + MIN_LENGTH);
                AnimationTimer.Start();
                animationStart = DateTime.Now;
                isRunning = true;
            }
            else if (!isRunning && punchQueue.Count == 0 && lastWidth > 0)
            {
                lastWidth -= decreasePerTick;
                if (lastWidth <= 0)
                {
                    lastWidth = 0;
                }
                Invoke((MethodInvoker) delegate
                {
                    bar.Width = lastWidth;
                });
            }
        }

        public void Cleanup()
        {
            InitializerTimer.Stop();
            if (AnimationTimer != null)
            {
                AnimationTimer.Stop();
            }
        }

        private void AnimationTimer_Tick(object sender, ElapsedEventArgs e)
        {
            TimeSpan span = e.SignalTime - animationStart;
            double percentTime = span.TotalMilliseconds / animationDuration.TotalMilliseconds;

            if (span >= animationDuration)
            {
                AnimationTimer.Stop();
                lastWidth = currWidth;
                decreasePerTick = Math.Max(lastWidth / 100, 1);
                isRunning = false;
            }

            float percentForce = current_Force / MAX_FORCE;

            Color forceColor = Tools.Interpolate(Low, Medium, High, percentForce);

            int width = (int)(lastWidth + (currWidth - lastWidth) * percentTime);

            Invoke((MethodInvoker)delegate
            {
                bar.Width = width;
            });
            bar.BackColor = Color.FromArgb(255, forceColor.R, forceColor.G, forceColor.B);
        }

        public void PushPunch(float force)
        {
            punchQueue.Enqueue(force);
        }

    }
}