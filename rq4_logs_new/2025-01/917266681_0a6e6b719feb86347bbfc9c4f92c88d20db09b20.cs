using System;
using System.IO;
using Google.Cloud.AIPlatform.V1;
using Google.Apis.Auth.OAuth2;
using System.Threading.Tasks;
using Google.Protobuf;
using System.Linq;
using Google.Api.Gax.Grpc;
using static Google.Cloud.AIPlatform.V1.AugmentPromptRequest.Types;
using System.Text;
//using static Google.Cloud.AIPlatform.V1.PredictionService;

namespace GeminiCLI
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Replace with your project ID, location, model name, and the path to your key file
            string projectId = "temp-a3r4ux9v-wsky";
            string location = "us-central1";
            string publisher = "google";
            string modelName = "gemini-1.5-flash-001";
            //string credentialsPath = "\\\\wsl.localhost\\Ubuntu\\home\\alexo\\.config\\gcloud\\application_default_credentials.json";

            string prompt = "What is the meaning of life?"; // Take input as command-line arguments

            // Create client
            var predictionServiceClient = new PredictionServiceClientBuilder
            {
                Endpoint = $"{location}-aiplatform.googleapis.com"
            }.Build();

            // Initialize content request
            var generateContentRequest = new GenerateContentRequest
            {
                Model = $"projects/{projectId}/locations/{location}/publishers/{publisher}/models/{modelName}",
                GenerationConfig = new GenerationConfig
                {
                    Temperature = 0.4f,
                    TopP = 1,
                    TopK = 32,
                    MaxOutputTokens = 2048
                },
                Contents =
                        {
                            new Content
                            {
                                Role = "USER",
                                Parts =
                                {
                                    new Part { Text = prompt },
                                    //new Part { Text = "What's in this photo?" },
                                    //new Part { FileData = new() { MimeType = "image/png", FileUri = "gs://generativeai-downloads/images/scones.jpg" } }
                                }
                            }
                        }
            };

            // Make the request, returning a streaming response
            using PredictionServiceClient.StreamGenerateContentStream response = predictionServiceClient.StreamGenerateContent(generateContentRequest);

            StringBuilder fullText = new();

            // Read streaming responses from server until complete
            AsyncResponseStream<GenerateContentResponse> responseStream = response.GetResponseStream();
            await foreach (GenerateContentResponse responseItem in responseStream)
            {
                fullText.Append(responseItem.Candidates[0].Content.Parts[0].Text);
            }

            Console.WriteLine(fullText.ToString());

            Console.ReadLine();
        }
    }
}