
using NotifyMe.Models.DbModels;
using NotifyMe.ServiceInterfaces;
using System;
using System.ComponentModel;
using System.Windows.Input;
using Xamarin.Forms;

namespace NotifyMe.ViewModels
{
    public class AccountViewModel : INotifyPropertyChanged
    {
        private IUserService _userService;

        private User _currentUser;

        public AccountViewModel(IUserService userService)
        {
            _userService = userService;
            _currentUser = _userService.GetCurrentUser();
            Name = _currentUser.Name;
            Email = _currentUser.Email;
            Telephone = _currentUser.Telephone;
        }

        public string Name { get; set; }

        public string Email { get; set; }

        public string Telephone { get; set; }

        public ICommand UpdateAccountPic
        {
            get
            {
                return new Command(()=> { });
            }
        }

        public ICommand ChangePassword 
        {
            get
            {
                return new Command(()=> { });
            }
        }

        public ICommand UpdateAccount
        {
            get
            {
                return new Command(async () => {
                    if (Name == null || Email == null )
                    {
                        await Application.Current.MainPage.DisplayAlert("Alert", "Name and Email should be there.", "Ok");
                    }
                    else
                    {
                        try
                        {
                            _currentUser.Name = Name;
                            _currentUser.Email = Email;
                            _currentUser.Telephone = Telephone;

                            _userService.UpdateUser(_currentUser);
                            _userService.SetCurrentUser(_currentUser);
                        }
                        catch (Exception)
                        {
                            //DB Error
                            await Application.Current.MainPage.DisplayAlert("Oops", "Can't connect to database.", "Ok");
                        }
                    }
                });
            }
        }

        #region INotifyPropertyChanged Implementation
        public event PropertyChangedEventHandler PropertyChanged;
        void OnPropertyChanged([System.Runtime.CompilerServices.CallerMemberName] string propertyName = "")
        {
            if (PropertyChanged == null)
                return;

            PropertyChanged.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
        #endregion
    }
}