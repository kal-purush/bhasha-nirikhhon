using System;
namespace XabluApp.Core
{
    public class WebViewViewModel : BaseViewModel
    {
        public void Init(string webUrl)
        {
            WebUrl = webUrl;
        }

        private string webUrl;
        public string WebUrl
        {
            get { return webUrl; }
            set { SetProperty(ref webUrl, value); }
        }
    }
}