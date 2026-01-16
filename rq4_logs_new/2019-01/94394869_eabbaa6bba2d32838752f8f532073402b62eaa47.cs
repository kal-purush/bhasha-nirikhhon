using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.Gms.Ads;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using Xamarin.Forms;
using Xamarin.Forms.Platform.Android;
using YanSoft.CurrencyExchanger.Core.Configurations;
using YanSoft.CurrencyExchanger.Core.Controls;
using YanSoft.CurrencyExchanger.Droid.Renderers;

[assembly: ExportRenderer(typeof(AdBanner), typeof(AdBannerRenderer))]
namespace YanSoft.CurrencyExchanger.Droid.Renderers
{
    public class AdBannerRenderer : ViewRenderer<AdBanner, AdView>
    {
        private AdView adControl;
        Context context;
        public AdBannerRenderer(Context _context) : base(_context)
        {
            context = _context;
        }

        protected override void OnElementChanged(ElementChangedEventArgs<AdBanner> e)
        {
            base.OnElementChanged(e);
            if (e.NewElement == null)
            {
                return;
            }
            if (Control == null)
            {
                adControl = CreateAdControl();
                //e.NewElement.HeightRequest = adControl.Height;
                SetNativeControl(adControl);
            }

            if (e.OldElement != null)
            {
                // Unsubscribe from event handlers and cleanup any resources
            }

            if (e.NewElement != null)
            {
                // Configure the control and subscribe to event handlers
            }
        }

        private AdView CreateAdControl()
        {
            adControl = new AdView(context)
            {
#if DEBUG
                AdUnitId = AppConfigurations.AdMobBannerTestAdUnitId,
#endif
#if !DEBUG
                AdUnitId = AppConfigurations.AdMobAndroidBannerAdUnitId,
#endif
                AdSize  = AdSize.SmartBanner
            };
            adControl.LoadAd(new AdRequest.Builder().Build());
            return adControl;
        }


    }
}