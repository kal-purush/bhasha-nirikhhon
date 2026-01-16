using System;
using System.Collections.Generic;
using System.Text;
using TaskDudes.Views;
using Xamarin.Forms;

namespace TaskDudes.ViewModels
{
    public class RegisterViewModel: BaseViewModel
    {
        public Command LoginCommand { get; }
        public Command RegisterCommand { get; }

        public RegisterViewModel()
        {
            LoginCommand = new Command(OnLoginClicked);
            RegisterCommand = new Command(OnRegisterClicked);
        }

        private async void OnLoginClicked(object obj)
        {
            // Prefixing with `//` switches to a different navigation stack instead of pushing to the active one
            await Shell.Current.GoToAsync($"//{nameof(MainPage)}");
        }

        private async void OnRegisterClicked(object obj)
        {
            // Prefixing with `//` switches to a different navigation stack instead of pushing to the active one
            await Shell.Current.GoToAsync($"//{nameof(LoginPage)}");
        }
    }
}