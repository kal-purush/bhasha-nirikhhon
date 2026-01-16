using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Android.App;
using Android.Content;
using Android.Widget;
using Android.OS;
using Android.Support.V7.App;
using Android.Views;
using OsmSharp.Android.UI;
using OsmSharp.Android.UI.Data.SQLite;
using OsmSharp.Math.Geo;
using OsmSharp.Osm;
using OsmSharp.Osm.API;
using OsmSharp.Osm.Data;
using OsmSharp.Osm.Data.Cache;
using OsmSharp.Osm.Data.Memory;
using OsmSharp.Osm.Xml.v0_6;
using OsmSharp.UI;
using OsmSharp.UI.Map;
using OsmSharp.UI.Map.Layers;
using OsmSharp.UI.Map.Styles;
using OsmSharp.UI.Map.Styles.MapCSS;
using OsmTest.Android.Model;
using OsmTest.Android.Services;
using Environment = Android.OS.Environment;

namespace OsmTest.Android
{
	[Activity (Label = "OsmTest", MainLauncher = true, Icon = "@mipmap/icon", Theme = "@style/Theme.AppCompat")]
	public class MainActivity : ActionBarActivity
   {
      /// <summary>
      /// Holds the mapview.
      /// </summary>
      private MapView _mapView;

	   ApiService _service = null;


      private CancellationTokenSource _cancellationTokenSource;
	   private string css = @"
node
{
    color:#03f;
    width:2;
    opacity:0.7;
    fill-color:#fc0;
    fill-opacity:0.3;
}
way
{
    color:#03f;
    width:5;
    opacity:0.6;
}
area
{
    color:#03f;
    width:2;
    opacity:0.7;
    fill-color:#fc0;
    fill-opacity:0.3;
}
relation node, relation way, relation relation
{
    color:#d0f;
}";


      protected async override void OnCreate(Bundle bundle)
      {
         base.OnCreate(bundle);
         //SetContentView(Resource.Layout.Main);
         _service = new ApiService();
         StyleInterpreter interpreter = null;
         try
         {
            //List<OsmGeo> osm = null;
            List<OsmGeo> osm = await _service.DownloadArea(35.878039, -9.465, 36.833, -9.077);
            if (osm != null)
            {
               Native.Initialize();

               // initialize map.
               var map = new Map();
               string path = Environment.ExternalStorageDirectory + Java.IO.File.Separator + "default_css.mapcss";
               Java.IO.File file = new Java.IO.File(path);
               string content = "";
               if (file.Exists())
               {
                  using (var streamReader = new StreamReader(path))
                  {
                     content = streamReader.ReadToEnd();
                     System.Diagnostics.Debug.WriteLine(content);
                  }
               }
               Stream csStream = Resources.OpenRawResource(Resource.Raw.default_css);
               interpreter = new MapCSSInterpreter(css);

               IDataSourceReadOnly source = new MemoryDataSource(osm.ToArray());
               var t = map.AddLayerOsm(source, interpreter);

               _mapView = new MapView(this, new MapViewSurface(this));
               _mapView.Map = map;
               _mapView.MapMaxZoomLevel = 30; // limit min/max zoom because MBTiles sample only contains a small portion of a map.
               _mapView.MapMinZoomLevel = 1;
               _mapView.MapTilt = 0;
               _mapView.MapCenter = new GeoCoordinate(36, -9.2);
               _mapView.MapZoom = 16;
               _mapView.MapAllowTilt = false;

               //OsmSharp.Data.SQLite.SQLiteConnectionBase sqLiteConnection = new SQLiteConnection("osmMap");

               Toast.MakeText(this, "Read success", ToastLength.Long).Show();
               SetContentView(_mapView);
            }
            else
            {
               Toast.MakeText(this, "OSM is null", ToastLength.Long).Show();
               SetContentView(Resource.Layout.Main);
            }
         }
         catch (Exception exception)
         {
            Toast.MakeText(this, exception.Message, ToastLength.Long).Show();
            SetContentView(Resource.Layout.Main);
         }
      }

	   protected async override void OnResume()
	   {
	      base.OnResume();
	   }

	   protected override void OnPause()
	   {
	      base.OnPause();
      }

	   public override bool OnCreateOptionsMenu(IMenu menu)
      {
         var inflater = MenuInflater;
         inflater.Inflate(Resource.Menu.action_items, menu);
         return true;
      }

      public override bool OnOptionsItemSelected(IMenuItem item)
      {
         switch (item.ItemId)
         {
            case Resource.Id.atn_direct_enable:
               UpdateFromService();
               return true;
            case Resource.Id.atn_direct_discover:
               return true;
            default:
               return base.OnOptionsItemSelected(item);
         }
      }

	   private async void UpdateFromService()
	   {
         ApiService service = new ApiService();

         string testData = await service.GetTestData();
         Toast toast = Toast.MakeText(this, testData, ToastLength.Long);
         toast.Show();
      }

	   //private async Task<MvxGeoLocation> UpdateLocation()
	   //{
	   //   _cancellationTokenSource?.Cancel();
	   //   _cancellationTokenSource = new CancellationTokenSource();
    //     var locationService = Mvx.Resolve<LocationService>();
	   //   return await locationService.GetLocation(_cancellationTokenSource);
	   //}
   }
}

