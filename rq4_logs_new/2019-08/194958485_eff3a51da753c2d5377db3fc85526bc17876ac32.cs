using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using Taskio.Droid;
using Xamarin.Forms;
[assembly: Dependency(typeof(PhotoPicker))]
namespace Taskio.Droid
{
    public class PhotoPicker : IPhotoPicker
    {
        public Task<Stream> GetImageStreamAsync()
        {
            //Defining intent for geting images
            Intent intent = new Intent();
            intent.SetType("image/*");
            intent.SetAction(Intent.ActionGetContent);
            //save the TaskCompletion object as a Mainactivity property
            MainActivity.Instance.PickImageTaskCompletionSource = new TaskCompletionSource<Stream>();

            //return task obj
            return MainActivity.Instance.PickImageTaskCompletionSource.Task;
        }
    }
}