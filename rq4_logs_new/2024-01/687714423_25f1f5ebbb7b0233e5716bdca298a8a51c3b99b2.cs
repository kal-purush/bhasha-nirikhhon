using System.Net;
using Ardalis.GuardClauses;
using EmbeddingEngine.Core;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.ChatCompletion;
using Microsoft.SemanticKernel.Connectors.OpenAI;

namespace Tlv.Recall
{
    public class Complete : SearchBase
    {
        private readonly ILogger _logger;

        public Complete(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<Complete>();
        }

        [Function(nameof(Complete))]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequestData req)
        {
            try
            {
                string? prompt = req.Query["q"];
                if (string.IsNullOrEmpty(prompt))
                {
                    var _response = req.CreateResponse(HttpStatusCode.BadRequest);
                    await _response.WriteStringAsync("Please provide some input, i.e. add ?q=... to invocation url");
                    return _response;
                }

                #region Read Configuration

                string openaiEndpoint = GetConfigValue("OPENAI_ENDPOINT");
                string openaiAzureKey = GetConfigValue("OPENAI_AZURE_KEY");
                string collectionName = GetConfigValue("COLLECTION_NAME");
                string vectorDbProviderKey = GetConfigValue("VECTOR_DB_PROVIDER_KEY");

                #endregion

                _logger.LogInformation($"Executing {nameof(Complete)} with prompt {prompt}");

                string? embeddingsProviderName = req.Query["p"] ?? "OPENAI";
                string configKeyName = $"{embeddingsProviderName.ToString().ToUpper()}_KEY";
                string? apiKey = GetConfigValue(configKeyName);

                configKeyName = $"{embeddingsProviderName.ToString().ToUpper()}_AZURE_KEY";
                string? azureApiKey = GetConfigValue(configKeyName);

                //var searchResuls = await Search(embeddingsProviderName, apiKey,
                //                                vectorDbProviderKey,
                //                                collectionName,
                //                                prompt);

                var searchResuls = await Search(apiKey,
                                                openaiEndpoint,
                                                collectionName,
                                                prompt);

                // Initialize the SK
                IKernelBuilder kernelbuilder = Kernel.CreateBuilder()
                    .AddAzureOpenAIChatCompletion("gpt4", // Azure OpenAI Deployment Name,
                                                 openaiEndpoint, openaiAzureKey);
                var kernel = kernelbuilder.Build();

                IChatCompletionService ai = kernel.GetRequiredService<IChatCompletionService>();
                Guard.Against.Null(ai);

                ChatHistory history = [];
                if (searchResuls.Count > 0)
                {
                    string? summary = searchResuls[0].summary;
                    if( !string.IsNullOrEmpty(summary) )
                        history.AddUserMessage(summary);
                }

                OpenAIPromptExecutionSettings openAIPromptExecutionSettings = new()
                {
                    ToolCallBehavior = ToolCallBehavior.AutoInvokeKernelFunctions,
                    Temperature = 0
                };

                var result = await ai.GetChatMessageContentAsync(history,
                                                 executionSettings: openAIPromptExecutionSettings);
                //ai.GetStreamingChatMessageContentsAsync();

                var response = req.CreateResponse(HttpStatusCode.OK);
                await response.WriteAsJsonAsync(response);
                return response;
            }
            catch (Exception ex)
            {
                var _response = req.CreateResponse(HttpStatusCode.InternalServerError);
                _response.WriteString(ex.Message);
                return _response;
            }
        }
    }
}