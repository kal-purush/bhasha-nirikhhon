using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using MvvmCross.Commands;
using Xamarin.Essentials;
using YanSoft.CurrencyExchanger.Core.Resources;

namespace YanSoft.CurrencyExchanger.Core.ViewModels
{
    public class AboutViewModel : BaseViewModel
    {
        //TODO
        private string shareUrl = "";
        private string feedbackEmail = "yan_xiaodi@hotmail.com";
        public AboutViewModel()
        {

        }

        #region Commands

        #region FeedbackAsyncCommand;
        private IMvxAsyncCommand _feedbackAsyncCommand;
        public IMvxAsyncCommand FeedbackAsyncCommand
        {
            get
            {
                _feedbackAsyncCommand = _feedbackAsyncCommand ?? new MvxAsyncCommand(FeedbackAsync);
                return _feedbackAsyncCommand;
            }
        }
        private async Task FeedbackAsync()
        {
            // Implement your logic here.
            try
            {
                var message = new EmailMessage
                {
                    Subject = "Currency Exchanger Feedback",
                    Body = "",
                    To = new List<string> { feedbackEmail },
                    //Cc = ccRecipients,
                    //Bcc = bccRecipients
                };
                await Email.ComposeAsync(message);
            }
            catch (FeatureNotSupportedException fbsEx)
            {
                // Email is not supported on this device
            }
            catch (Exception ex)
            {
                // Some other exception occurred
            }
        }
        #endregion


        #region ShareAsyncCommand;
        private IMvxAsyncCommand _shareAsyncCommand;
        public IMvxAsyncCommand ShareAsyncCommand
        {
            get
            {
                _shareAsyncCommand = _shareAsyncCommand ?? new MvxAsyncCommand(ShareAsync);
                return _shareAsyncCommand;
            }
        }
        private async Task ShareAsync()
        {
            // Implement your logic here.
            await Share.RequestAsync(new ShareTextRequest
            {
                Text = $"{AppResources.About_Share_Content} {shareUrl}",
            });
        }
        #endregion
        #endregion
    }
}