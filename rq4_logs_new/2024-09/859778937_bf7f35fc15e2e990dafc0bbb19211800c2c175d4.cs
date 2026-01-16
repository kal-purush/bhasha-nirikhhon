using AITest.Helpers;
using AITest.Services;
using Azure.AI.Vision.ImageAnalysis;
using Azure.Identity;
using Hackathon.AI.Models.Settings;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.ChatCompletion;
using Microsoft.SemanticKernel.Connectors.OpenAI;
using System.Net;

namespace AITest.Controllers;

[ApiController]
[Route("api/[controller]")]
public class VisionController : ControllerBase
{
    private readonly Kernel _kernel;
    private readonly IChatCompletionService _chatCompletionService;
    private readonly ChatHistoryService _chatHistoryService;
    private readonly IOptions<OpenAISettings> _settings;
    private readonly AzureVisionHelper _vision;

    public VisionController(ChatHistoryService chatHistoryService,IOptions<OpenAISettings> settings, AzureVisionHelper vision)
    {
        _settings = settings;
        _vision = vision;
    }


    [HttpPost]
    public IActionResult Post([FromBody]FileStream image)
    {
        ImageAnalysisResult result = _vision.AnalyseImage(image);
        return Ok(result);
    }
}