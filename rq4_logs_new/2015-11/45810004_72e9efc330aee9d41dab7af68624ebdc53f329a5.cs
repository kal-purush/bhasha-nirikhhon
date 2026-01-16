using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Windows.Devices.Power;
using Windows.System.Power;

namespace Doze.Models
{
    public class BatteryModel : INotifyPropertyChanged
    {
        
        private int _BatteryCapacity;
        public int BatteryCapacity
        {
            get { return _BatteryCapacity; }
            set
            {
                if ( value != _BatteryCapacity) _BatteryCapacity = value;
                OnPropertyChanged("BatteryCapacity");
            }
        }


        private int _RemainingCapacity;
        public int RemainingCapacity
        {
            get { return _RemainingCapacity; }
            set
            {
                if (value != _RemainingCapacity) _RemainingCapacity = value;
                OnPropertyChanged("RemainingCapacity");
            }
        }


        private int _HealthyBatteryCapacity;
        public int HealthyBatteryCapacity
        {
            get { return _HealthyBatteryCapacity; }
            set
            {
                if (value != _HealthyBatteryCapacity) _HealthyBatteryCapacity = value;
                OnPropertyChanged("HealthyBatteryCapacity");
            }
        }


        private int _ChargeRate;
        public int ChargeRate
        {
            get { return _ChargeRate; }
            set
            {
                if (value != _ChargeRate) _ChargeRate = value;
                OnPropertyChanged("ChargeRate");
            }
        }

        private BatteryStatus _Status;
        public BatteryStatus Status
        {
            get { return _Status; }
            set
            {
                if (value != _Status) _Status = value;
                OnPropertyChanged("Status");
            }
        }

        public BatteryModel(int BattryCapacity , int RemaingCapacity , int Chargerate , BatteryStatus status , int HealthyBatterycapacity)
        {
            BatteryCapacity = BattryCapacity;
            RemainingCapacity = RemaingCapacity;
            ChargeRate = Chargerate;
            Status = status;
            HealthyBatteryCapacity = HealthyBatterycapacity;

        }

        #region Notify Property Changed Members
        public event PropertyChangedEventHandler PropertyChanged;
        private void OnPropertyChanged( string propertyName )
        {
            PropertyChangedEventHandler handler = PropertyChanged;
            if (handler != null)
            {
                handler(this, new PropertyChangedEventArgs(propertyName));
            }
        }
        #endregion
    }
}