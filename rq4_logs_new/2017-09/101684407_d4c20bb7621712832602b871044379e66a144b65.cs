using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using static ToyTrainProject.Shared.AppConfiguration;
using defaults = ToyTrainProject.Shared.AppConfiguration.ComputerVisionAPI.OCR;

namespace ToyTrainProject.Models
{
    internal class ComputerVision_OCR : AnalyticsWrapper
    {
        public ComputerVision_OCR() : base(SubscriptionKey, UriBase, "application/octet-stream")
        {
        }

        public async Task<string> callService(System.Drawing.Bitmap bitMap, string language, bool detectOrientation)
        {
            var queryString = HttpUtility.ParseQueryString(string.Empty);

            queryString["language"] = language;
            queryString["detectOrientation"] = detectOrientation.ToString();

            var body = new System.Drawing.ImageConverter().ConvertTo(bitMap, typeof(byte[])) as byte[];

            HttpResponseMessage response = await callEndpoint("ocr", queryString, body);
            var responseString = await response.Content.ReadAsStringAsync();

            return responseString;
        }

        public async Task<string> callService(System.Drawing.Bitmap bitMap) => await callService(bitMap, defaults.language, defaults.detectOrientation);
    }
}