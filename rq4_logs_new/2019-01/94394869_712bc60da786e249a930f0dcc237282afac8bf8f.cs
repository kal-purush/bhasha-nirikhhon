using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Advertising.WinRT.UI;
using Windows.UI.Xaml;
using Xamarin.Forms.Platform.UWP;
using YanSoft.CurrencyExchanger.Core.Controls;
using YanSoft.CurrencyExchanger.UWP.Renderers;

[assembly: ExportRenderer(typeof(AdBanner), typeof(AdBannerRenderer))]
namespace YanSoft.CurrencyExchanger.UWP.Renderers
{
    public class AdBannerRenderer :ViewRenderer<AdBanner, AdControl>
    {
        public AdBannerRenderer()
        {

        }

        protected override void OnElementChanged(ElementChangedEventArgs<AdBanner> e)
        {
            base.OnElementChanged(e);
            if(e.NewElement == null)
            {
                return;
            }
            if(Control == null)
            {
                var adControl = new AdControl
                {
#if DEBUG
                    ApplicationId = "3f83fe91-d6be-434d-a0ae-7351c5a997f1",
                    AdUnitId = "test",
#endif
#if !DEBUG
                    ApplicationId = "9wzdncrdq91s",
                    AdUnitId = "1100038490"
#endif
                };
                var availableWidth = Window.Current.Bounds.Width;
                if (availableWidth >= 728)
                {
                    adControl.Width = 728;
                    adControl.Height = 90;
                }
                else if (availableWidth >= 640)
                {
                    adControl.Width = 640;
                    adControl.Height = 100;
                }
                else if (availableWidth >= 320)
                {
                    adControl.Width = 320;
                    adControl.Height = 50;
                }
                else if (availableWidth >= 300)
                {
                    adControl.Width = 300;
                    adControl.Height = 50;
                }


                e.NewElement.HeightRequest = adControl.Height;


                SetNativeControl(adControl);

            }
        }
    }
}