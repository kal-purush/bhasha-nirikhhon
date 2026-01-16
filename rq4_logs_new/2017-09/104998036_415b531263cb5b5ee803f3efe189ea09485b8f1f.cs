using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;


using SixLabors.Fonts;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.Drawing;
using SixLabors.Primitives;

namespace CustomerFeedback
{

    class GenerateCoupon {
        public async static Task<HttpResponseMessage> Run(HttpRequestMessage req, CloudBlockBlob inputCoupon, CloudBlockBlob outputBlob, TraceWriter log)
        {
            
            // Get request body
            var json = req.Content.ReadAsStringAsync().Result;
            dynamic data = JsonConvert.DeserializeObject(json);

            // Set name to body data
            string name = data?.CustomerName;
            
            log.Info("Received coupon request for: " + name);
            
            // Generate Coupon Id
            Random _rdm = new Random();
            var randomCode = _rdm.Next(1000,9999);
            
            // Get Coupon image and write new coupon
            using (Stream inputMemoryStream = new MemoryStream())
            using (Stream outputMemoryStream = new MemoryStream()){
            
                // Get the coupon image
                await inputCoupon.DownloadToStreamAsync(inputMemoryStream);
                inputMemoryStream.Position = 0;
                
                // Write the text
                WriteWatermark("25% off voucher\n" + name +"\n" + randomCode, inputMemoryStream, outputMemoryStream,log);
                
                // Write to blob
                outputMemoryStream.Position = 0;
                await outputBlob.UploadFromStreamAsync(outputMemoryStream);
                
                outputBlob.Properties.ContentType = "image/jpeg";
                await outputBlob.SetPropertiesAsync();
            }

            // Return
            return new HttpResponseMessage()
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(JsonConvert.SerializeObject(new { CouponUrl = GetBlobSasUri(outputBlob) }))
            };
        }

        private static void WriteWatermark(string watermarkContent, Stream originalImage, Stream newImage, TraceWriter log)
        {
            log.Info("Reading image");
            var inputImage = Image.Load(originalImage);

			float targetWidth = inputImage.Width - 10;
			float targetHeight = inputImage.Height - 10;

            log.Info("Creating font");
            // Create font
			Font font = SystemFonts.CreateFont("Arial", 10);

			// Measure the text size
			SizeF size = TextMeasurer.Measure(watermarkContent, new RendererOptions(font));

			// Find out how much we need to scale the text to fill the space (up or down)
			float scalingFactor = Math.Min(inputImage.Width / size.Width, inputImage.Height / size.Height);

			// Create a new font 
			Font scaledFont = new Font(font, scalingFactor * font.Size);

			var center = new PointF(inputImage.Width / 2, inputImage.Height / 2);

			log.Info("Writing watermark");
			inputImage.Mutate(i => i.DrawText(watermarkContent, scaledFont, Rgba32.Black, center, new TextGraphicsOptions(true)
			{
				HorizontalAlignment = HorizontalAlignment.Center,
				VerticalAlignment = VerticalAlignment.Center
			}));

			log.Info("Writing to the output stream");
            inputImage.SaveAsJpeg(newImage);
		}

        private static string GetBlobSasUri(CloudBlockBlob blob)
        {
            
            //Set the expiry time and permissions for the blob.
            //In this case the start time is specified as a few minutes in the past, to mitigate clock skew.
            //The shared access signature will be valid immediately.
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy();
            sasConstraints.SharedAccessStartTime = DateTime.UtcNow.AddMinutes(-5);
            sasConstraints.SharedAccessExpiryTime = DateTime.UtcNow.AddHours(24);
            sasConstraints.Permissions = SharedAccessBlobPermissions.Read;

            //Generate the shared access signature on the blob, setting the constraints directly on the signature.
            string sasBlobToken = blob.GetSharedAccessSignature(sasConstraints);

            //Return the URI string for the container, including the SAS token.
            return blob.Uri + sasBlobToken;
        }
    }
}