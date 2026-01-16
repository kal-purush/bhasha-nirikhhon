using Android.App;
using Android.Content;
using Android.Gms.Maps;
using Android.Gms.Maps.Model;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using AndroidX.AppCompat.App;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PieShopMobile.Resources.layout
{
    [Activity(Label = "MapActivity")]
    public class MapActivity : AppCompatActivity, IOnMapReadyCallback
    {
     
        private readonly LatLng _pieShopLocation = new LatLng(-33.88897419071586, 18.514112692539168);
        GoogleMap _googleMap;

        public void OnMapReady(GoogleMap map)
        {
            _googleMap = map;

            _googleMap.UiSettings.ZoomControlsEnabled = true;
            _googleMap.UiSettings.CompassEnabled = true;
            _googleMap.UiSettings.MyLocationButtonEnabled = false;
            AddMarker();
        }

        protected override void OnCreate(Bundle bundle)
        {
            base.OnCreate(bundle);
            SetContentView(Resource.Layout.shop_map);

            var mapFragment = (MapFragment)FragmentManager.FindFragmentById(Resource.Id.map);
            mapFragment.GetMapAsync(this);
        }

        void AddMarker()
        {
            var marker = new MarkerOptions();
            marker.SetPosition(_pieShopLocation)
                               .SetTitle("Pie Shop");
            _googleMap.AddMarker(marker);

            var cameraUpdate = CameraUpdateFactory.NewLatLngZoom(_pieShopLocation, 15);
            _googleMap.MoveCamera(cameraUpdate);
        }
    }
}