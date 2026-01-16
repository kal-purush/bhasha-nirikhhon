using System.ComponentModel;
using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;

using NotifyMe.Models;
using NotifyMe.Views;

namespace NotifyMe.ViewModels
{
    public class HomePageMasterViewModel : INotifyPropertyChanged
    {
        public ObservableCollection<HomePageMenuItem> MenuItems { get; set; }

        public HomePageMasterViewModel()
        {
            MenuItems = new ObservableCollection<HomePageMenuItem>(new[]
            {
                    new HomePageMenuItem { Id = 0, Title = "Page 1", TargetType = typeof(HomePageDetail) },
                    new HomePageMenuItem { Id = 1, Title = "Page 2", TargetType = typeof(HomePageDetail) },
                    new HomePageMenuItem { Id = 2, Title = "Page 3", TargetType = typeof(HomePageDetail) },
                    new HomePageMenuItem { Id = 3, Title = "Page 4", TargetType = typeof(HomePageDetail) },
                    new HomePageMenuItem { Id = 4, Title = "Page 5", TargetType = typeof(HomePageDetail) },
            });
        }

        #region INotifyPropertyChanged Implementation
        public event PropertyChangedEventHandler PropertyChanged;
        void OnPropertyChanged([CallerMemberName] string propertyName = "")
        {
            if (PropertyChanged == null)
                return;

            PropertyChanged.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
        #endregion
    }
}