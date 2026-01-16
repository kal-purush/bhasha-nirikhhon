using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.Media;
using Android.OS;
using Android.Runtime;
using Android.Support.V4.App;
using Android.Views;
using Android.Widget;
using Xamarin.Forms;

namespace NotifyMe.Droid
{
    [BroadcastReceiver]
    public class NotificationSender : BroadcastReceiver
    {
        public override void OnReceive(Context context, Intent intent)
        {
            var ID = intent.GetIntExtra("ID", 0);
            var Title = intent.GetStringExtra("Title");
            var Body = intent.GetStringExtra("Body");

            //Intent notificationTapIntent = new Intent(Forms.Context, typeof());

            Notification.Builder notificationBuilder = new Notification.Builder(Android.App.Application.Context);

            notificationBuilder.SetContentTitle(Title)
                               .SetContentText(Body)
                               .SetSmallIcon(Resource.Drawable.abc_ic_menu_paste_mtrl_am_alpha)
                               .SetDefaults(NotificationDefaults.Sound | NotificationDefaults.Vibrate)
                               .SetPriority((int)NotificationPriority.Max);

            NotificationManagerCompat notificationManager = NotificationManagerCompat.From(Android.App.Application.Context);
            notificationManager.Notify(ID, notificationBuilder.Build());
        }

    }
}