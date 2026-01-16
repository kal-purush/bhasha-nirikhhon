using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BasicLib;
using Client.Infrastructure;
using Client.ViewModel;
using System.ComponentModel;

namespace Client.Model
{
    class ClientCharacter : BasicLib.Character, INotifyPropertyChanged, IDisposable
    {
        public ClientCharacter()
        {
        }
        public ClientCharacter(Character ch):base()
        {
            this.FirstName = ch.FirstName;
        }
        private RelayCommand summonCharacter_Info;

        public RelayCommand SummonCharacter_Info
        {
            get
            {
                if (summonCharacter_Info == null)
                {
                    summonCharacter_Info = new RelayCommand(ExecuteSummonCharacter_Info);
                }
                return summonCharacter_Info;
            }
        }

        private void ExecuteSummonCharacter_Info(object param)
        {
            WindowViewLoaderService.Show(typeof(CharacterWindowViewModel));
        }

        public event PropertyChangedEventHandler PropertyChanged;

        public virtual void OnPropertyChanged(string propertyName)
        {
            PropertyChangedEventHandler handler = this.PropertyChanged;
            if (handler != null)
            {
                handler.Invoke(this, new PropertyChangedEventArgs(propertyName));
            }
        }
        public void Dispose()
        {
            this.OnDispose();
        }
        protected virtual void OnDispose()
        {

        }
    }
}