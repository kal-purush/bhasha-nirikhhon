using MineSearch.Common.ViewModels;

namespace DigitalDisplay.Wpf.ViewModels
{
    public class MainWindowViewModel : ViewModelBase
    {
        public uint GameDurationSeconds
        {
            get
            {
                return _gameDurationSeconds;
            }
            set
            {
                if (_gameDurationSeconds != value)
                {
                    _gameDurationSeconds = value;
                    OnPropertyChanged();
                }
            }
        }

        private uint _gameDurationSeconds;
    }
}