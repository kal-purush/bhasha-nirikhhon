using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Foundation;
using Google.MobileAds;
using UIKit;
using Xamarin.Forms;
using Xamarin.Forms.Platform.iOS;
using YanSoft.CurrencyExchanger.Core.Configurations;
using YanSoft.CurrencyExchanger.Core.Controls;
using YanSoft.CurrencyExchanger.iOS.Renderers;

[assembly: ExportRenderer(typeof(AdBanner), typeof(AdBannerRenderer))]
namespace YanSoft.CurrencyExchanger.iOS.Renderers
{
    public class AdBannerRenderer : ViewRenderer<AdBanner, BannerView>
    {
        private BannerView adControl;
        

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

        private BannerView CreateAdControl()
        {
            adControl = new BannerView(AdSizeCons.SmartBannerPortrait)
            {
#if DEBUG
                AdUnitID = AppConfigurations.AdMobIosBannerTestAdUnitId,
#endif
#if !DEBUG
                AdUnitId = AppConfigurations.AdMobIosAppId,
#endif
            };
            adControl.RootViewController = GetRootViewController();
            adControl.LoadRequest(Request.GetDefaultRequest());
            return adControl;
        }

        private UIViewController GetRootViewController()
        {
            foreach (UIWindow window in UIApplication.SharedApplication.Windows)
            {
                if (window.RootViewController != null)
                {
                    return window.RootViewController;
                }
            }
            return null;
        }

    }
}