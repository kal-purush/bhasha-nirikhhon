using System;
using System.ComponentModel;
using System.Linq;
using Android.Content;
using Android.Gms.Maps;
using Android.Gms.Maps.Model;
using Android.Graphics;
using Xamarin.Forms;
using Xamarin.Forms.Maps;
using Xamarin.Forms.Maps.Android;
using Xamarin.Forms.Platform.Android;
using System.Net;
using MovilFinalCrea.Models;

namespace MovilFinalCrea.Droid
{
    public class MapWithIconControlRenderer: MapRenderer
    {
        private  MapWithIconControl formsMaP;
        private  GoogleMap nativeMap;

        public MapWithIconControlRenderer(Context context) : base(context)
        {

        }

        protected override void OnElementChanged(ElementChangedEventArgs<Map> e)
        {
            base.OnElementChanged(e);
            if (e.NewElement != null)
            {
                this.formsMaP = (MapWithIconControl)e.NewElement;
            }
        }

        protected override void OnElementPropertyChanged(object sender, PropertyChangedEventArgs e)
        {
            base.OnElementPropertyChanged(sender, e);
            switch (e.PropertyName)
            {
                case nameof(MapWithIconControl.IconProperty):
                case nameof(MapWithIconControl.Icon):
                case nameof(MapWithIconControl.Pins):
                    IconPropertyChanged();
                    break;
                default:
                    break;

            }
        }


        protected override void OnMapReady(GoogleMap map)
        {
            base.OnMapReady(map);
            IconPropertyChanged();
        }


        private void IconPropertyChanged()
        {
            if (this.nativeMap == null)
            {
                return;
            }
            if (!formsMaP.Pins?.Any() ?? true)
            {
                return;
            }

            MarkerOptions markerOptions = new MarkerOptions();
            markerOptions.SetPosition(new LatLng(formsMaP.Pins[0].Position.Latitude, formsMaP.Pins[0].Position.Longitude));
            markerOptions.SetIcon(BitmapDescriptorFactory.FromBitmap(GetImageBitmapFromUrl(this.formsMaP.Icon)));


            Device.BeginInvokeOnMainThread(() =>
            {
                try
                {
                    this.formsMaP.Pins.Clear();
                    this.nativeMap.AddMarker(markerOptions);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message, ex);
                }
            });

        }


        private Bitmap GetImageBitmapFromUrl(String url)
        {

            Bitmap imageBitmap = null;
            using (WebClient webClient = new WebClient())
            {
                byte[] imageBytes = webClient.DownloadData(url);
                if (imageBytes != null && imageBytes.Length > 0)
                {
                    imageBitmap = BitmapFactory.DecodeByteArray(imageBytes, 0, imageBytes.Length);
                }
            }
            return imageBitmap;
        }


    }
}