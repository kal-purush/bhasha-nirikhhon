using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using PdfSharpCore.Drawing;
using PdfSharpCore.Pdf;
using Azure.Storage.Blobs;

namespace PdfTester
{
    public static class FxPdfSharpGenerator
    {
        [FunctionName("FxPdfSharpGenerator")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;

            var document = new PdfDocument();
            var page = document.AddPage();
            var gfx = XGraphics.FromPdfPage(page);
            var font = new XFont("OpenSans", 20, XFontStyle.Bold);

            gfx.DrawString(name, font, XBrushes.Black, new XRect(20, 20, page.Width, page.Height), XStringFormats.Center);

            //Stream file = new Stream();
            System.IO.Stream stream = new System.IO.MemoryStream();
            document.Save(stream);

            string cloudStorageAccountConnectionString = Environment.GetEnvironmentVariable("AzureStorageConnectionString");



            BlobContainerClient container = new BlobContainerClient(cloudStorageAccountConnectionString, "pdftest");
            var blobClient = container.GetBlobClient($"{name}.pdf");



        

            blobClient.Upload(stream, overwrite: true);


            return new OkObjectResult(blobClient.Uri.ToString());

            
        }
    }
}