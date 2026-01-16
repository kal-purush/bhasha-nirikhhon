using Base64Universal.Common;
using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Input;
#if WINDOWS_PHONE_APP
using Windows.ApplicationModel.Email;
#endif

namespace Base64Universal.ViewModels
{
    public class AboutPageViewModel
    {
        public ICommand ContactCommand
        { get; private set; }

        public AboutPageViewModel()
        {
            ContactCommand = new RelayCommand(Contact);
        }

        private async void Contact()
        {
#if WINDOWS_PHONE_APP
            var emailMessage = new EmailMessage
            {
                Subject = "Windows Phone 'Base Converter' feedback"
            };
            emailMessage.To.Add(new EmailRecipient(Constants.MyEmail, Constants.GeorgeAidonidis));
            // call EmailManager to show the compose UI in the screen
            await EmailManager.ShowComposeNewEmailAsync(emailMessage);
#endif
        }
    }
}