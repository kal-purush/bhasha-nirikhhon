using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Runtime.InteropServices;
using System.Threading;

namespace FramerateStabilizer
{
    public static class StableFrames
    {
        /// <summary>
        /// Used in a loop to variably increase processing time in order to normalize it
        /// </summary>
        /// <param name="millisecondsPerFrame">Loop's maximum time per iteration</param>
        /// <param name="timer">A timer that is already started</param>
        public static void Stabilize(long millisecondsPerFrame, Stopwatch timer)
        {
            long frameCount = timer.ElapsedMilliseconds / millisecondsPerFrame;
            double sleepTime = (millisecondsPerFrame - (timer.ElapsedMilliseconds - (millisecondsPerFrame * frameCount)));
            Thread.Sleep((int)sleepTime);
        }
    }
}

