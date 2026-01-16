using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ZorgPortalIoT.Forms
{
    public partial class TemplateForm
    {
        private CancellationTokenSource StopSource { get; set; }

        /// <summary>
        /// Running property.
        /// </summary>
        /// <value>
        /// Tracks if the clock is running.
        /// </value>
        public bool Running { get; private set; }

        /// <summary>
        /// Interval in milliseconds.
        /// </summary>
        /// <value>
        /// The amount of milliseconds the clock waits before resfreshing data.
        /// </value>
        private readonly int interval = 10000;

        /// <summary>
        /// Main async loop that refreshes data each interval.
        /// </summary>
        private async void RealtimeClock()
        {
            while (!StopSource.IsCancellationRequested)
            {
                //Run refresh function
                await Task.Run(RefreshData);

                Console.WriteLine($"Refresh activated on object: {this.ToString()}");

                //Cancel delay to stop task on request
                try
                {
                    //Delay for interval milliseconds
                    await Task.Delay(interval, StopSource.Token);
                }
                catch (TaskCanceledException)
                {
                    break;
                }

            }
        }

        /// <summary>
        /// Starts the clock.
        /// </summary>
        public void StartRefresh()
        {
            //Ensure previous instances are stopped before running a new one
            if (StopSource != null)
            {
                StopRefresh();
            }

            //Set new CancellationSource to stop process later and run a new data generator
            StopSource = new CancellationTokenSource();
            Task.Run(RealtimeClock, StopSource.Token);

            Running = true;
        }

        /// <summary>
        /// Stops the clock.
        /// </summary>
        public void StopRefresh()
        {
            try
            {
                StopSource.Cancel();
                StopSource.Dispose();
            }
            catch (ObjectDisposedException)
            {
                Console.WriteLine("An attempt was made to stop a clock that was not running.");
            }

            Running = false;
        }

        /// <summary>
        /// Function that gets called every interval, it reshreshes the data of the form.
        /// </summary>
        virtual public void RefreshData() 
        {
            
        }
    }
}