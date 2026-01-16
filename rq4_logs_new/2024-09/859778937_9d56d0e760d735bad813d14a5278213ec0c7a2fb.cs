using Azure.AI.Vision.ImageAnalysis;
using Azure;
using Microsoft.Extensions.Options;
using Hackathon.AI.Models.Settings;

namespace AITest.Helpers
{
    public class AzureVisionHelper
    {
        private readonly OpenAISettings _settings;
        public AzureVisionHelper(IOptions<OpenAISettings> settings)
        {
            _settings = settings.Value;
        }
        public ImageAnalysisResult AnalyseImage(string imageUrl)
        {
            string endpoint = _settings.Endpoint;
            string key = _settings.Key;

            ImageAnalysisClient client = new ImageAnalysisClient(
                new Uri(endpoint),
                new AzureKeyCredential(key));

            ImageAnalysisResult result = client.Analyze(
                new Uri(imageUrl),
                VisualFeatures.Caption | VisualFeatures.Read,
                new ImageAnalysisOptions { GenderNeutralCaption = true });

            return result;
            //Console.WriteLine("Image analysis results:");
            //Console.WriteLine(" Caption:");
            //Console.WriteLine($"   '{result.Caption.Text}', Confidence {result.Caption.Confidence:F4}");

            //Console.WriteLine(" Read:");
            //foreach (DetectedTextBlock block in result.Read.Blocks)
            //    foreach (DetectedTextLine line in block.Lines)
            //    {
            //        Console.WriteLine($"   Line: '{line.Text}', Bounding Polygon: [{string.Join(" ", line.BoundingPolygon)}]");
            //        foreach (DetectedTextWord word in line.Words)
            //        {
            //            Console.WriteLine($"     Word: '{word.Text}', Confidence {word.Confidence.ToString("#.####")}, Bounding Polygon: [{string.Join(" ", word.BoundingPolygon)}]");
            //        }
            //    }
        }
    }
}