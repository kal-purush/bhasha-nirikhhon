using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.Graphics;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using OsmDroid;
using OsmDroid.Api;
using OsmDroid.Util;
using OsmDroid.Views;
using OsmDroid.Views.Overlay;

namespace OsmTest.Android.Views
{
   public class GeoPositionOverlay : Overlay
   {
      public EventHandler<IGeoPoint> GeoPointTapped;
      public GeoPositionOverlay(IntPtr javaReference, JniHandleOwnership transfer) : base(javaReference, transfer)
      {
      }

      public GeoPositionOverlay(Context p0) : base(p0)
      {
      }

      public GeoPositionOverlay(IResourceProxy p0) : base(p0)
      {
      }

      public override void Draw(Canvas p0, MapView p1, bool p2)
      {
      }

      public override bool OnSingleTapUp(MotionEvent p0, MapView p1)
      {
         var x = p0.GetX();
         var y = p0.GetY();
         IProjection proj = p1.Projection;
         IGeoPoint p = proj.FromPixels((int)p0.GetX(), (int)p0.GetY());
         GeoPointTapped?.Invoke(this, p);
         return base.OnSingleTapUp(p0, p1);
      }
   }
}