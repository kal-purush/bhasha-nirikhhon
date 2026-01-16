using AzurePipelinesToGitHubActionsConverter.Core;
using Microsoft.AspNetCore.Mvc;

namespace AzurePipelinesToGitHubActionsConverter.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class ConvertController : ControllerBase
{
    private readonly Conversion _converter;

    public ConvertController()
    {
        _converter = new Conversion(verbose: false);
    }

    [HttpPost("pipeline")]
    public IActionResult ConvertPipeline([FromBody] PipelineConversionRequest request)
    {
        if (string.IsNullOrWhiteSpace(request?.AzurePipelineYaml))
            return BadRequest("Azure pipeline YAML must be provided.");

        var result = _converter.ConvertAzurePipelineToGitHubAction(request.AzurePipelineYaml, addWorkFlowDispatch: true);

        var response = new PipelineConversionResponse
        {
            OriginalYaml = result.pipelinesYaml,
            ConvertedYaml = result.actionsYaml,
            Comments = result.comments ?? new List<string>()
        };

        return Ok(response);
    }
}

public class PipelineConversionRequest
{
    public string AzurePipelineYaml { get; set; } = null!;
}

public class PipelineConversionResponse
{
    public string OriginalYaml { get; set; } = null!;
    public string ConvertedYaml { get; set; } = null!;
    public List<string> Comments { get; set; } = new();
}