using System;
using Android.Content;
using Laurus.Mileage.Droid;
using Laurus.Mileage.Services;
using Xamarin.Forms;

[assembly: Dependency(typeof(EmailHelper))]
namespace Laurus.Mileage.Droid
{
    public class EmailHelper : IEmailHelper
    {
        public EmailHelper()
        {
        }

        public void SendEmail(string attachment)
        {
            var email = new Intent(Android.Content.Intent.ActionSend);
            //email.PutExtra(Android.Content.Intent.ExtraEmail,
            //    new string[] { "person1@xamarin.com", "person2@xamrin.com" });

            //email.PutExtra(Android.Content.Intent.ExtraCc,
            //new string[] { "person3@xamarin.com" });

            email.PutExtra(Android.Content.Intent.ExtraSubject, "Expense Report");

            email.PutExtra(Android.Content.Intent.ExtraText, "Expense report from MileageApp");
            var file = new Java.IO.File(attachment);
            file.SetReadable(true, true);
            var uri = Android.Net.Uri.FromFile(file);
            email.PutExtra(Intent.ExtraStream, uri);

            email.SetType("message/rfc822");


            Forms.Context.StartActivity(Intent.CreateChooser(email, "Send email"));
        }
    }
}