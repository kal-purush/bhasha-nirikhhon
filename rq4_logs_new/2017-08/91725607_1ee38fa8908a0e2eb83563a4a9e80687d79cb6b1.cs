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
using App1;

namespace UGYM
{
    [Activity(Label = "ListActivity")]
    public class ListActivity : Activity
    {

        public int rutina = 0;
        private ListView days;

        protected override void OnCreate(Bundle savedInstanceState)
        {
            base.OnCreate(savedInstanceState);
            SetContentView(Resource.Layout.DayList);

            days = FindViewById<ListView>(Resource.Id.lv_days);

            days.

            // Create your application here
        }
    }
}