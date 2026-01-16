using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Threading;

namespace Project_WPM.classes
{
    public class Timer : INotifyPropertyChanged
    {
        DispatcherTimer tmr = new DispatcherTimer();

        private string count;
        public string Count
        {
            get
            {
                return count;
            }
            set
            {
                count = value;
                OnPropertyChanged("Count");
                if (Int32.Parse(count) <= 0)
                {
                    tmr.Stop();
                }
            }
        }

        public event PropertyChangedEventHandler PropertyChanged;
        public void OnPropertyChanged(string propertyHP)
        //바꼈을때 바꼈다고 신호를 전달해줄 함수
        {
            var handler = PropertyChanged;
            //이벤트 델리게이트
            if (handler != null)
            {
                handler(this, new PropertyChangedEventArgs(propertyHP));
                //메인윈도우(this)에서 SCORE(propertyScore)가 변경되었다
            }
        }
        public Timer()
        {
            Count = "0";
            tmr.Interval = new TimeSpan(0, 0, 1);
            tmr.Tick += new EventHandler(tmr_Tick);
        }

        public void setTime(int second)
        {
            Count = second.ToString();
        }
        public void stop()
        {
            tmr.Stop();
        }

        public void start()
        {
            tmr.Start();
        }

        void tmr_Tick(object sender, EventArgs e)
        {
            if (Int32.Parse(Count) > 0)
            {
                Count = (Int32.Parse(count) - 1).ToString();
            }
            else
            {
                tmr.Stop();
            }
        }
    }
}