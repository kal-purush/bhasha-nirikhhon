using NotifyMe.ServiceInterfaces;
using NotifyMe.Services;
using NotifyMe.Views;
using System;
using System.Windows.Input;
using Xamarin.Forms;

namespace NotifyMe.ViewModels
{
    public class LoginViewModel
    {
        private UserService _userService;

        public LoginViewModel()
        {
            _userService = new UserService();
        }

        public string Email { get; set; }

        public string Password { get; set; }

        public ICommand Login
        {
            get
            {
                return new Command(() => {
                    if (Email != null && Password != null)
                    {
                        try
                        {
                            var user = _userService.GetUserByEmail(Email);

                            if (user != null)
                            {
                                var password = user.Password;
                                if (password == Password)
                                {
                                    //Login success
                                }
                                else
                                {

                                }
                            }
                            else //invalid Email
                            {

                            }
                        }
                        catch (Exception e)
                        {
                            //DB Error
                        }
                    }
                    else //Incomplete inputs
                    {
                        
                    }
                });
            }
        }

        public ICommand Signup
        {
            get
            {
                return new Command(() => {
                    Application.Current.MainPage.Navigation.PushAsync(new SignupPage());
                });
            }
        }
    }
}