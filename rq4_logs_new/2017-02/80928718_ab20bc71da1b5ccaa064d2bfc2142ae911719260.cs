using Android.OS;
using MvvmCross.Core.ViewModels;
using MvvmCross.Droid.Support.V7.AppCompat;

namespace XabluApp.Droid.Activities
{
    public abstract class BaseActivity<TViewModel> : BaseActivity where TViewModel : class, IMvxViewModel
    {
        public new TViewModel ViewModel
        {
            get { return (TViewModel)base.ViewModel; }
            set { base.ViewModel = value; }
        }
    }

    public abstract class BaseActivity : MvxCachingFragmentCompatActivity
    {
        protected abstract int FragmentId { get; }

        protected override void OnCreate(Bundle bundle)
        {
            base.OnCreate(bundle);
            this.Window.AddFlags(Android.Views.WindowManagerFlags.DrawsSystemBarBackgrounds);
            this.Window.ClearFlags(Android.Views.WindowManagerFlags.TranslucentStatus);
            this.Window.SetStatusBarColor(Resources.GetColor(Resource.Color.colorPrimaryDark));
            SetContentView(FragmentId);
        }
    }
}