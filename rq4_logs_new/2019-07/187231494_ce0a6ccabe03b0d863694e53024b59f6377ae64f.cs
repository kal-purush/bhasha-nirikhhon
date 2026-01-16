using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Webkit;
using Android.Widget;
using iotc_xamarin_ble.Droid.Helpers;
using iotc_xamarin_ble.Services.Cookies;
using Xamarin.Forms;

[assembly: Dependency(typeof(Cookies))]
namespace iotc_xamarin_ble.Droid.Helpers
{
    public class Cookies : ICookies
    {
        public void Clear()
        {
            var cookieManager = CookieManager.Instance;
            cookieManager.RemoveAllCookie();
        }
    }
}