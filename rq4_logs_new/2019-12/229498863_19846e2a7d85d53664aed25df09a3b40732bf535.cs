using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using Xamarin.Forms;

namespace FormsMvvm2019
{
    /// <summary>
    /// 画面遷移処理を一元化する.
    /// </summary>
    public class AppCoordinator
    {
        readonly ILogger logger;

        public AppCoordinator()
        {
            logger = AppLogger.GetLogger(this);
        }

        public void Initialize()
        {
            MessagingCenter.Subscribe<AlertEventArgs>(this, Constants.MessageDisplayAlert, async (s) =>
            {
                logger.LogInformation($"MessageDisplayAlert {s.Title} {s.Message}");
                await Application.Current.MainPage.DisplayAlert(s.Title, s.Message, s.Cancel);
            });

            Application.Current.MainPage = new NavigationPage(new SplashPage());
        }
    }

    /// <summary>
    /// DisplayAlert用イベント引数.
    /// </summary>
    public class AlertEventArgs : EventArgs
    {
        public string Title { get; set; }
        public string Message { get; set; }
        public string Cancel { get; set; }

        /// <summary>
        /// コンストラクタ.
        /// 投げっぱなし用.
        /// </summary>
        /// <param name="title"></param>
        /// <param name="message"></param>
        /// <param name="cancel"></param>
        public AlertEventArgs(string title, string message, string cancel)
        {
            Title = title;
            Message = message;
            Cancel = cancel;
        }
    }
}