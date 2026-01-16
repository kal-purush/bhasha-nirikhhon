using AITest.Helpers;
using AITest.Services;
using Azure.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.OpenApi;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.ChatCompletion;
using Microsoft.SemanticKernel.Connectors.OpenAI;
using System.Net;

namespace AITest.Controllers;

[ApiController]
[Route("api/[controller]")]
public class ChatController : ControllerBase
{
    private readonly Kernel _kernel;
    private readonly IChatCompletionService _chatCompletionService;
    private readonly ChatHistoryService _chatHistoryService;

    public ChatController(ChatHistoryService chatHistoryService)
    {
        DelegatingHandler handler = new ApiVersionHandler("2023-03-15-preview");
        var client = new HttpClient();
        IKernelBuilder kernelBuilder = Kernel.CreateBuilder();
        kernelBuilder.AddAzureOpenAIChatCompletion(
            deploymentName: "gpt-4o"
            , apiKey: "403a8f4f895d4326bfd04e6a5676c226"
            , endpoint: "https://hackathon20240402694327.openai.azure.com/"
            , modelId: "gpt-4o"
            , httpClient: client
            );

        _kernel = kernelBuilder.Build();
        _chatCompletionService = _kernel.GetRequiredService<IChatCompletionService>();
        _chatHistoryService = chatHistoryService;
    }

    [HttpPost]
    public async Task<IActionResult> Post(string prompt = "describe this image in detail", string imageUri = "https://upload.wikimedia.org/wikipedia/commons/6/62/Panthera_tigris_sumatran_subspecies.jpg", int maxTokens = 50)
    {
        string sessionId = "default-session";
        ChatHistory history = _chatHistoryService.GetOrCreateHistory(sessionId);

        _chatHistoryService.AddUserMessage(prompt, imageUri, sessionId);

        OpenAIPromptExecutionSettings openAIPromptExecutionSettings = new OpenAIPromptExecutionSettings
        {
            ToolCallBehavior = ToolCallBehavior.AutoInvokeKernelFunctions,
            MaxTokens = maxTokens
        };

        ChatMessageContent result = await _chatCompletionService.GetChatMessageContentAsync(history, executionSettings: openAIPromptExecutionSettings, kernel: _kernel);

        history.AddAssistantMessage(result.Content ?? string.Empty);

        return new JsonResult(new { reply = result.Content });
    }
}