using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Train_UI
{
    //This struct is only used when automatically running a clock.
    //You can customize how much you want a clock to move forward per tick
    public struct TickInterval
    {
        public int secondTick;  
        public int minuteTick;
        public int hourTick;

        public TickInterval(int hour, int minute, int second)
        {
            secondTick = second;
            minuteTick = minute;
            hourTick = hour;
        }
    }

    //This class represents a virtual clock that runs in the background of the system
    //and keeps track of the current date and the current time of the day. This is the primary
    //background system that governs how the simulation runs
    class Clock
    {
        private int hour;  //hours in current day
        private int minute; //minutes in current day
        private int second; //seconds in current day
        private int day; //current day number
        DateTime date;  //stores the current time and date for the clock

        //Description: Constructor for Clock
        //Pre-Condition: None
        //Post-Condition: New clock object made
        public Clock()
        {
            hour = 0;
            minute = 0;
            second = 0;
            day = 0;
            date = new DateTime(2019, 1, 1);
        }

        //Description: Moves clock forward one second
        //Pre-Condition: None
        //Post-Condition: Time value is moved up on second
        public void Tick()
        {
            second++;
            if (second == 60)
            {
                minute++;
                second = 00;
            }

            if (minute == 60)
            {
                hour++;
                minute = 00;
            }

            if (hour == 24)
            {
                day++;
                hour = 00;
            }
        }

        //Description: Progresses clock by given amount, instead
        //of one second you can progress the clock more per tick
        //such as 1 hour or 30 minutes per tick, etc.
        //Pre-Condition: None
        //Post-Condition:  Progresses time value by given input amount
        public void CustomTick(int hour, int minute, int second)
        {
            //following statements update the second, minute, and hour field
            this.second = second;
            if (this.second >= 60)
            {
                this.minute += this.second / 60;
                this.second %= 60;
            }

            this.minute = minute;

            if (this.minute >= 60)
            {
                this.hour += this.minute / 60;
                this.minute %= 60;
            }

            this.hour = hour;
            if (this.hour >= 24)
            {
                day += hour / 24;
                this.hour %= 24;
            }

            //updates the clock date by adding the fields calculated above
            date = date.AddDays(day);
            date = date.AddHours(this.hour);
            date = date.AddMinutes(this.minute);
            date = date.AddSeconds(this.second);

            day = 0; //this is to prevent moving forward more than one day at a time
        }


        //Description: Returns the current date and time
        //Pre-Condition: None
        //Post-Condition: Returns current date and time as a DateTime object
        public DateTime GetTime()
        {
            DateTime temp = new DateTime(date.Year, date.Month, date.Day, date.Hour, date.Minute, date.Second);
            return temp;
        }

        //Description: Displays current time
        //Pre-Condition: None
        //Post-Condition: Displays time as "month/day/year hour:minute:second"
        public string DisplayTime()
        {
            string time = "Time: " + date;
            return time;
        }

        //Description: Continuously runs the clock as a thread
        //Pre-Condition: None
        //Post-Condition: Updates and displays each custom clock tick
        public async void RunClock(TickInterval interval)
        {
            while (true)
            {
                CustomTick(interval.hourTick, interval.minuteTick, interval.secondTick);
                Console.WriteLine(DisplayTime());
                await Task.Delay(1000);
                Console.Clear();
            }
        }
    }
}