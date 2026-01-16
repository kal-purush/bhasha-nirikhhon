using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using Google.Cloud.Vision.V1;

namespace OCREngine
{
    public class VisionAPIReader
    {
        public string GetImageText(byte[] image)
        {
            string path = HttpContext.Current.Server.MapPath("~/") + "\\PriceComparisonEngine-6efc463cda92.json";

            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", path);

            var client = ImageAnnotatorClient.Create();

            var imageToRead = Google.Cloud.Vision.V1.Image.FromBytes(image);

            var response = client.DetectDocumentText(imageToRead);

            return response.Text;
        }
    }
}