using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Util;
using Android.Views;
using Android.Widget;
using EWiki.XF.Droid.LocationFeeder.Helper;
using EWiki.XF.Droid.LocationFeeder.Repository;
using EWiki.XF.Droid.Services;
using EWiki.XF.Utilities;
using Microsoft.AspNet.SignalR.Client;
using Newtonsoft.Json;
using Xamarin.Forms;

namespace EWiki.XF.Droid.BackgroundServices
{
    [Service]
    public class SniperStatusCheckingBackgroundService : Service
    {
        private NotificationService _notificationService = new NotificationService();

        private CancellationTokenSource _cts;

        private readonly HubConnection _connection;
        private readonly IHubProxy _proxy;

        public SniperStatusCheckingBackgroundService()
        {
            _connection = new HubConnection("http://signalr.pokit.mobi/");
            _proxy = _connection.CreateHubProxy("SniperHub");

            _proxy.On("GetSniperStatus", (string message) =>
            {
                if (!string.IsNullOrEmpty(message))
                {
                    try
                    {
                        var sniperStatus = JsonConvert.DeserializeObject<SniperStatus>(message).IsAvailable;
                        var currentSniperStatus = LocalDataStorage.GetSniperStatus();

                        if (sniperStatus != currentSniperStatus)
                        {
                            LocalDataStorage.SaveSniperStatus(sniperStatus);
                            _notificationService.Notify("Pokit", sniperStatus ? "Sniper is available" : "Sniper is temporarily unavailable", 0);
                        }
                    }
                    catch (Exception)
                    {
                        // Suppress?
                    }
                }
            });
        }

        public override IBinder OnBind(Intent intent)
        {
            return null;
        }

        public override StartCommandResult OnStartCommand(Intent intent, StartCommandFlags flags, int startId)
        {
            _cts = new CancellationTokenSource();

            Task.Run(async () =>
            {
                try
                {
                    while (!_cts.IsCancellationRequested)
                    {
                        await _proxy.Invoke("CheckSniperStatus");

                        Log.Info("", "Reconnecting to stream in 5 mins");
                        Thread.Sleep(300000);
                    }
                }
                catch (Android.OS.OperationCanceledException)
                {
                    // Suppress?
                }
                finally
                {
                    if (_cts.IsCancellationRequested)
                    {
                        var message = new CancelledMessage();
                        Device.BeginInvokeOnMainThread(
                            () => MessagingCenter.Send(message, "CancelledMessage")
                            );
                    }
                }
            }, _cts.Token);

            return StartCommandResult.Sticky;
        }
    }
}