using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PieShopMobile
{
    [Activity(Label = "AboutActivity")]
    public class AboutActivity : Activity
    {
        private Button _phoneButton;
        protected override void OnCreate(Bundle savedInstanceState)
        {
            base.OnCreate(savedInstanceState);
            SetContentView(Resource.Layout.about);
            FindViews(); 
            LinkEventHandlers();

            // Create your application here
        }
        private void LinkEventHandlers()
        {
            _phoneButton.Click += PhoneButton_Click;
        }

        private void PhoneButton_Click(object sender, EventArgs e)
        {
            var intent = new Intent(Intent.ActionDial);
            intent.SetData(Android.Net.Uri.Parse("tel:+27749288137"));
            intent.AddFlags(ActivityFlags.NewTask);
            StartActivity(intent);
        }

        private void FindViews()
        {
            _phoneButton = FindViewById<Button>(Resource.Id.phoneButton);
            
        }
    }
}