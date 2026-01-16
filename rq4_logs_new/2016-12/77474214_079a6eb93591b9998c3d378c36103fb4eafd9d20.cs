using System;

using Android.App;
using Android.Content.PM;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using Android.OS;
using Octane.Xam.VideoPlayer.Android;

namespace AppNavTest.Droid
{
    [Activity(Label = "AppNavTest", Icon = "@drawable/icon", Theme = "@style/MainTheme", MainLauncher = true, ConfigurationChanges = ConfigChanges.ScreenSize | ConfigChanges.Orientation)]
    public class MainActivity : global::Xamarin.Forms.Platform.Android.FormsAppCompatActivity
    {
        protected override void OnCreate(Bundle bundle)
        {
            TabLayoutResource = Resource.Layout.Tabbar;
            ToolbarResource = Resource.Layout.Toolbar;
            AppDomain.CurrentDomain.UnhandledException += (sender, args) => {
                //handle the exception here

            };
            AndroidEnvironment.UnhandledExceptionRaiser += (s, e) =>
            {
                e.Handled = true;
            };
            base.OnCreate(bundle);

          

            global::Xamarin.Forms.Forms.Init(this, bundle);
          
            FormsVideoPlayer.Init(null);
            LoadApplication(new App());
        }
    }
}
