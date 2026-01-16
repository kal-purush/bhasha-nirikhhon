using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DependencyInversionPrincple
{
    class Program
    {
        static void Main(string[] args)
        {
            var facebookAPI = new FacebookAPI();
            var twitterAPI = new TwitterAPI();
            var application = new WebApplication(twitterAPI);

            application.shareArticle();

            Console.ReadLine();
        }
    }

    class WebApplication
    {
        private IAPI api;

        public WebApplication(IAPI api)
        {
            this.api = api;
        }

        public void shareArticle()
        {
            var title = Console.ReadLine();
            var message = Console.ReadLine();

            api.share(title, message);
        }
    }

    interface IAPI
    {
        void share(String title, String message);
    }
}