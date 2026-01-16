using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MailSender.lib.MVVM;

namespace TestWPF.ViewModels
{
    internal class MainWindowViewModel: ViewModel
    {
        private string _Title = "Наше новое окно!";
        public string Title
        {
            get => _Title;
            set {
                if (Set(ref _Title, value))
                    OnPropertyChanged(nameof(TitleLength));
            }
            //set
            //{
            //    if (Equals(_Title, value)) return;
            //    _Title = value;
            //    OnPropertyChanged("Title");
            //    OnPropertyChanged();
            //}
        }
        public int TitleLength => _Title.Length;

        private int _Angle = 0;
        public int Angle
        {
            get => _Angle;
            set => Set(ref _Angle, value);
        }

        private int _X = 0;
        public int X
        {
            get => _X;
            set => Set(ref _X, value);
        }

        private int _Y = 0;
        public int Y
        {
            get => _Y;
            set => Set(ref _Y, value);
        }
    }
}