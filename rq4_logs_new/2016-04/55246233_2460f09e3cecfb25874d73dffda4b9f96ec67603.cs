using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sim
{
    class Metadata
    {
        int submitted;
        int completed;

        int response;
        int execution;
        int wait;
        int io;

        int burstMarker;

        public Metadata(int currentTime)
        {
            submitted = currentTime;
            execution = 0;
            wait = 0;
            io = 0;

            response = -1;

            burstMarker = currentTime;
        }

        public int UpdateLog(state currentState, int currentTime)
        {
            int prevBurstDuration = getDuration(currentTime);
            burstMarker = currentTime;  // reassign burstmarker to start tracking the next burst/wait period

            if (currentState == state.ready)
            {
                wait += prevBurstDuration;
            }
            else if (currentState == state.running)
            {
                execution += prevBurstDuration;
            }
            else if (currentState == state.io)
            {
                io += prevBurstDuration;
            }
            return prevBurstDuration; // add an error report
        }

        private int getDuration(int currentTime)
        {
            return currentTime - burstMarker;
        }







        public int getResponse()
        {
            return response;
        }
        public void setResponse(int currentTime)
        {
            response = currentTime - submitted;
        }

    }
}