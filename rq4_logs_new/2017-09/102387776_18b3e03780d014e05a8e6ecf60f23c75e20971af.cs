using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace CoolBytes.WebAPI.ViewModels
{
    public class Photo
    {
        public string PhotoUriPath { get; set; }
        public string PhotoUri { get; set; }

        public void FormatPhotoUri(IConfiguration configuration)
        {
            if (PhotoUriPath == null)
                return;

            var scheme = configuration["PhotosUri:Scheme"];
            var host = configuration["PhotosUri:Host"];
            var port = int.Parse(configuration["PhotosUri:Port"]);

            if (port == 80)
                PhotoUri = $"{scheme}://{host}{PhotoUriPath}";

            PhotoUri = $"{scheme}://{host}:{port}{PhotoUriPath}";
        }
    }
}