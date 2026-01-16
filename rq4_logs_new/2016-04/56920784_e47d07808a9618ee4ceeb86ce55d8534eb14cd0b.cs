using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.Locations;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;

namespace OsmTest.Android.Services
{
   public class CustomLocationProvider : Org.Osmdroid.Views.Overlay.Mylocation.GpsMyLocationProvider
   {
      private MainActivity _activity;

      public CustomLocationProvider(IntPtr javaReference, JniHandleOwnership transfer) : base(javaReference, transfer)
      {
      }

      public CustomLocationProvider(MainActivity activity) : base(activity)
      {
         _activity = activity;
      }

      public override void OnLocationChanged(Location p0)
      {
         base.OnLocationChanged(p0);
         _activity.ChangeCenterPoint(p0);
      }
   }
}