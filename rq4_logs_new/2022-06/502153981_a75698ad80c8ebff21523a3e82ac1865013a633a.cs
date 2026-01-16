using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace PikTestPlugin.Models
{
    public class ErrorModel : INotifyPropertyChanged
    {
        public ErrorModel(string errorMessage)
        {
            _errorMessage = errorMessage;
        }

        private string _infoMessage = "Произошла непредвиденная ошибка.\nТекст ошибки доступен ниже";
        private string _errorMessage = "Error";
        private string _buttonText = "Ok";

        public event PropertyChangedEventHandler PropertyChanged;
        public string InfoMessage 
        {
            get { return _infoMessage; }
            set 
            {
                _infoMessage = value; 
                OnPropertyChanged(nameof(InfoMessage));
            }
        }
        public string ErrorMessage
        {
            get { return _errorMessage; }
            set
            {
                _errorMessage = value;
                OnPropertyChanged(nameof(ErrorMessage));
            }
        }
        public string ButtonText
        {
            get { return _buttonText; }
            set
            {
                _buttonText = value;
                OnPropertyChanged(nameof(ButtonText));
            }
        }

        public void OnPropertyChanged([CallerMemberName] string prop = "")
        {
            if (PropertyChanged != null)
                PropertyChanged(this, new PropertyChangedEventArgs(prop));
        }
    }
}