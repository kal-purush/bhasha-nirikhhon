using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;

using NotifyMe.Droid;
using NotifyMe.ServiceInterfaces;

[assembly: Xamarin.Forms.Dependency(typeof(ToastAndroid))]
namespace NotifyMe.Droid
{
    public class ToastAndroid : IToastService
    {
        public void LongMessage(string message)
        {
            Toast.MakeText(Application.Context, message, ToastLength.Long).Show();
        }

        public void ShortMessage(string message)
        {
            Toast.MakeText(Application.Context, message, ToastLength.Short).Show();
        }
    }
}