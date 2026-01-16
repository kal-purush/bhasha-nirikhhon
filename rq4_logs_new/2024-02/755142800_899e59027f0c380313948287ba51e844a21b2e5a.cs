using Application.IServices;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers;

[Route("options")]
public class OptionsController : BaseController
{
    private readonly IOptionService _optionService;

    public OptionsController(IOptionService optionService)
    {
        _optionService = optionService;
    }
    
    /*[Authorize]*/
    [HttpGet("list")]
    public async Task<IActionResult> OptionsListAsync(int pageNumber, int pageSize,
        CancellationToken cancellationToken)
    {
        var result = await _optionService.GetOptionsPages(pageNumber, pageSize, cancellationToken);
        return Ok(result);
    }
    
    /*[Authorize]*/
    [HttpGet("list/{questionId}")]
    public async Task<IActionResult> OptionsListByTestIdAsync(Guid questionId, int pageNumber, int pageSize,
        CancellationToken cancellationToken)
    {
        var result = await _optionService.GetOptionsByTestIdPages(questionId, pageNumber, pageSize, cancellationToken);
        return Ok(result);
    }
}