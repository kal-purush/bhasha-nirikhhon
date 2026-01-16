using Mobile_Score.Droid;
using Mobile_Score.Services.Provider;
using System.Net.Http;
using Xamarin.Android.Net;
using Xamarin.Forms;

[assembly: Dependency(typeof(AndroidHttpMessageHandlerProvider))]
namespace Mobile_Score.Droid
{
    internal class AndroidHttpMessageHandlerProvider : INativeHttpMessageHandlerProvider
    {
        public AndroidHttpMessageHandlerProvider()
        {
        }

        public HttpMessageHandler Get()
        {
            return new AndroidClientHandler();
        }
    }
}