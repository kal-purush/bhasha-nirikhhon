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
    public class DailyActivityModel : INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler PropertyChanged;

        private String _myName;
        private String _myStabilityRate;
        private String _name0, _name1, _name2;
        private String _stability0, _stability1, _stability2;

        public String MyStabilityRate
        {
            get { return _myStabilityRate; }
            set { _myStabilityRate = "You have improved your stability by " + value + "%"; OnPropertyChanged("MyStabilityRate"); }
        }

        public String MyName
        {
            get { return _myName; }
            set { _myName = value + ", You are doing great !"; OnPropertyChanged("MyName"); }
        }

        public String UserName0
        {
            get { return _name0; }
            set { _name0 = value; OnPropertyChanged("UserName0"); }
        }

        public String UserName1
        {
            get { return _name1; }
            set { _name1 = value; OnPropertyChanged("UserName1"); }
        }
        public String UserName2
        {
            get { return _name2; }
            set { _name2 = value; OnPropertyChanged("UserName2"); }
        }

        public String Stability0
        {
            get { return _stability0; }
            set { _stability0 = value + "%"; OnPropertyChanged("Stability0"); }
        }
        public String Stability1
        {
            get { return _stability1; }
            set { _stability1 = value + "%"; OnPropertyChanged("Stability1"); }
        }
        public String Stability2
        {
            get { return _stability2; }
            set { _stability2 = value + "%"; OnPropertyChanged("Stability2"); }
        }

        public DailyActivityModel()
        {
            MyStabilityRate= "12";
            MyName = "Linda";
            UserName0 = "Saul";
            UserName1 = "Linda";
            UserName2 = "Kim";
            Stability0 = "20";
            Stability1 = "12";
            Stability2 = "8";
        }


        [NotifyPropertyChangedInvocator]
        protected virtual void OnPropertyChanged([CallerMemberName] string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}