using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using StaySteady.Mobile.Annotations;

namespace StaySteady.Mobile.Models
{
    public class AddReminderModel : INotifyPropertyChanged
    {
        public AddReminderModel()
        {
            ReminderText = "";
            ReminderDateTime = DateTime.Now;
        }

        public String ReminderText { get; set; }

        public DateTime ReminderDateTime { get; set; }

        public DateTime Date
        {
            get
            {
                return ReminderDateTime.Date;
            }
            set
            {
                ReminderDateTime = new DateTime(value.Year,value.Month,value.Day, ReminderDateTime.Hour,ReminderDateTime.Minute,0);
                OnPropertyChanged();
            }
        }

        public TimeSpan Time
        {
            get
            {
                return ReminderDateTime.TimeOfDay;
            }
            set
            {
                ReminderDateTime = new DateTime(ReminderDateTime.Year, ReminderDateTime.Minute, ReminderDateTime.Day, value.Hours, value.Minutes, 0);
                OnPropertyChanged();
            }
        }

        public event PropertyChangedEventHandler PropertyChanged;

        [NotifyPropertyChangedInvocator]
        protected virtual void OnPropertyChanged([CallerMemberName] string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}