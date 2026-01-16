using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;

namespace App1.Activitys
{
    [Activity(Label = "Rutinas")]
    public class RutinActivity : ListActivity
    {

        ListView rutinas;
        string[] items;
        

        protected override void OnCreate(Bundle savedInstanceState)
        {
            base.OnCreate(savedInstanceState);
            SetContentView(Resource.Layout.RutinList);

            // Create your application here

            rutinas = FindViewById<ListView>(Resource.Id.lv_rutinas);
            items = new string[] { "Día 1", "Día 2", "Día 3", "Día 4", "Día 5"};
            ListAdapter = new ArrayAdapter<String>(this, Android.Resource.Layout.SimpleListItem1, items);
            
        }

        protected override void OnListItemClick(ListView l, View v, int position, long id)
        {
            var t = items[position];
            Android.Widget.Toast.MakeText(this, t, Android.Widget.ToastLength.Short).Show();
        }

    }
}