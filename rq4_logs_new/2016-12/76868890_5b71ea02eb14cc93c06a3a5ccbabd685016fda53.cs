using Android.App;
using Android.Content.PM;
using Android.OS;
using FormsPlugin.Iconize.Droid;
using Microsoft.Practices.Unity;
using Plugin.Iconize.Fonts;
using Prism.Unity;

namespace OnePiece.App.Droid
{
    [Activity(Label = "OnePiece.App", Icon = "@drawable/icon", Theme = "@style/MainTheme", MainLauncher = true, ConfigurationChanges = ConfigChanges.ScreenSize | ConfigChanges.Orientation)]
    public class MainActivity : global::Xamarin.Forms.Platform.Android.FormsAppCompatActivity
    {
        protected override void OnCreate(Bundle bundle)
        {
            TabLayoutResource = Resource.Layout.Tabbar;
            ToolbarResource = Resource.Layout.Toolbar;

            base.OnCreate(bundle);

            FFImageLoading.Forms.Droid.CachedImageRenderer.Init();
            IconControls.Init(Resource.Id.toolbar, Resource.Id.sliding_tabs);
            Plugin.Iconize.Iconize.With(new MaterialModule()).With(new TypiconsModule());

            global::Xamarin.Forms.Forms.Init(this, bundle);
            LoadApplication(new App(new AndroidInitializer()));
        }
    }

    public class AndroidInitializer : IPlatformInitializer
    {
        public void RegisterTypes(IUnityContainer container)
        {

        }
    }
}
