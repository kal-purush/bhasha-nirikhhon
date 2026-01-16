using static System.Math;

namespace TeleprompterConsole
{
    /// <summary>
    /// Configuration class to handle the text display delay interaction between the keyboard input and ShowTeleprompter
    /// </summary>
    internal class TelePrompterConfig
    {
        public int DelayInMilliseconds { get; private set; } = 200;

        /// <summary>
        /// Updates the display delay
        /// </summary>
        /// <param name="increment">The degree of delay change, may be positive for an speed increase, or negative to slow down the text return</param>
        public void UpdateDelay(int increment) // Negative to speed up
        {
            var newDelay = Min(DelayInMilliseconds + increment, 1000);
            newDelay = Max(newDelay, 20);
            DelayInMilliseconds = newDelay;
        }

        public bool Done { get; private set; }

        public void SetDone()
        {
            Done = true;
        }
    }
}