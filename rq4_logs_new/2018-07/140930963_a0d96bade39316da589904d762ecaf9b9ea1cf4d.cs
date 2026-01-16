using NotifyMe.ServiceInterfaces;
using Rg.Plugins.Popup.Services;
using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Input;
using Xamarin.Forms;

namespace NotifyMe.ViewModels
{
    public class PasswordPopupViewModel
    {
        private IUserService _userService;

        public string CurrentPassword { get; set; }

        public string NewPassword { get; set; }

        public string ConfirmPassword { get; set; }

        public PasswordPopupViewModel(IUserService userServise)
        {
            _userService = userServise;
        }

        public ICommand Update
        {
            get
            {
                return new Command(async ()=> {
                    if (CurrentPassword == null || CurrentPassword.Equals(""))
                    {
                        await Application.Current.MainPage.DisplayAlert("Alert", "Current password is required.", "Ok");
                    }
                    else if (NewPassword == null || NewPassword.Equals(""))
                    {
                        await Application.Current.MainPage.DisplayAlert("Alert", "New password is required.", "Ok");
                    }
                    else if (ConfirmPassword == null || ConfirmPassword.Equals(""))
                    {
                        await Application.Current.MainPage.DisplayAlert("Alert", "Password confirmation is required.", "Ok");
                    }
                    else if (NewPassword != ConfirmPassword)
                    {
                        await Application.Current.MainPage.DisplayAlert("Alert", "Passwords mismatch", "Ok");
                    }
                    else
                    {
                        try
                        {
                            var user = _userService.GetCurrentUser();
                            var currentPwd = _userService.GetUserByEmail(user.Email).Password;

                            if (currentPwd == CurrentPassword)
                            {
                                user.Password = NewPassword;
                                _userService.UpdateUser(user);
                                await Application.Current.MainPage.DisplayAlert("Success", "Password successfully updated.", "Ok");
                                await PopupNavigation.PopAsync(true);
                            }
                            else
                            {
                                await Application.Current.MainPage.DisplayAlert("Alert", "Current passwords is invalid", "Ok");
                            }
                        }
                        catch (Exception)
                        {
                            await Application.Current.MainPage.DisplayAlert("Oops", "Can't connect to database.", "Ok");
                        }
                    }
                });
            }
        }
    }
}