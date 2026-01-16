using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace mySync.Jobs
{
    class Job
    {
        public JobDesc mParentDesc = null;
        private System.Timers.Timer timerObj = null;


        public float    progress        { get { return 0.0f;    } }
        public bool     isActive        { get { return (timerObj == null) || timerObj.Enabled;    } }


        public Job(JobDesc parent)
        {
            mParentDesc = parent;
        }

        public void Start()
        {
            if(timerObj != null)
            {
                timerObj.Stop();
                timerObj.Dispose();
            }

            timerObj = new System.Timers.Timer(10 * 1000);

            timerObj.Elapsed += OnTimedEvent;
            timerObj.AutoReset = false;
            timerObj.Enabled = true;
        }

        private void OnTimedEvent(Object source, ElapsedEventArgs e)
        {
            timerObj.Stop();
            timerObj.Dispose();
            timerObj = null;
        }

    }
}