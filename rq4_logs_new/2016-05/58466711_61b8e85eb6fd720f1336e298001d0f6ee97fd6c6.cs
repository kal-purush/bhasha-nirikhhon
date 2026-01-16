using System;
using XamarinTimeTracker.Model;

namespace XamarinTimeTracker.Model
{
    public class ProjectTimer : ITimer
    {

        public event EventHandler<DateTime> Elapsed;

        public void Start(TimeSpan interval)
        {
            Xamarin.Forms.Device.StartTimer(interval, () =>
            {
                Elapsed?.Invoke(this, Now);

                return true;
            });
        }

        public DateTime Now
        {
            get
            {
                return DateTime.UtcNow;
            }
        }

    }
}
