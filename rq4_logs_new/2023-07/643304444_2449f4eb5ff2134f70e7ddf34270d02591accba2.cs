namespace KamranWali.CodeOptPro.Timers
{
    public interface ITimer
    {
        /// <summary>
        /// This method sets up the timer.
        /// </summary>
        /// <param name="timeSeconds">The time to set in seconds, of type float</param>
        public void StartSetup(float timeSeconds);

        /// <summary>
        /// This method updates the timer and also includes the Time.deltaTime calculation.
        /// </summary>
        /// <param name="simSpeed">The simulation speed, of type float</param>
        public void UpdateTimer(float simSpeed);

        /// <summary>
        /// This method resets the timer.
        /// </summary>
        public void ResetTimer();

        /// <summary>
        /// This method stops the timer.
        /// </summary>
        public void StopTimer();

        /// <summary>
        /// This method checks if the timer is done.
        /// </summary>
        /// <returns>The flag that checks if the timer is done, of type bool</returns>
        public bool IsTimerDone();

        /// <summary>
        /// This method returns the time second value set for this timer.
        /// </summary>
        /// <returns>The time second value, of type float</returns>
        public float GetSetupTimeSeconds();

        /// <summary>
        /// This method returns the current time of the timer in seconds.
        /// </summary>
        /// <returns>The current time in seconds, of type float</returns>
        public float GetCurrentTime();

        /// <summary>
        /// The normalized value of the Timer.
        /// </summary>
        /// <returns>The normalized value of the Timer from 0 - 1, of type float</returns>
        float GetNormalValue();
    }
}