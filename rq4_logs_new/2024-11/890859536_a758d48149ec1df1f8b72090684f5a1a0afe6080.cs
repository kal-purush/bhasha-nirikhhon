using CosmeticsStore.Repositories.Models.Domain;
using CosmeticsStore.Services.DTO.Request;
using CosmeticsStore.Services.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace CosmeticsStore.API.Controllers;

[ApiController]
[Route("api/[controller]")]
public class CategoryController : ControllerBase
{
    private readonly ICategoryService _categoryService;

    public CategoryController(ICategoryService categoryService)
    {
        _categoryService = categoryService;
    }

    [HttpGet]
    [Route("{id:int}")]
    [ActionName(nameof(GetByIdAsync))]
    public async Task<IActionResult> GetByIdAsync(int id)
    {
        var serviceResponse = await _categoryService.GetByIdAsync(id);
        if (!serviceResponse.Succeeded)
        {
            return StatusCode(serviceResponse.StatusCode, new { status = serviceResponse.Status, details = serviceResponse.Details });
        }

        return Ok(new { status = serviceResponse.Status, details = serviceResponse.Details });
    }

    [HttpPost]
    public async Task<IActionResult> CreateAsync([FromBody] CategoryCreateDTO category)
    {
        var serviceResponse = await _categoryService.CreateAsync(category);
        if (!serviceResponse.Succeeded)
        {
            return StatusCode(serviceResponse.StatusCode, new { status = serviceResponse.Status, details = serviceResponse.Details });
        }

        return Ok(new { status = serviceResponse.Status, details = serviceResponse.Details });
    }
}