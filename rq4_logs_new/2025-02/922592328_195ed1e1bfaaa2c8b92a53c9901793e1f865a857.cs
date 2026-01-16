using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Monitoring.Models;
using Monitoring.Models.MonitoringModule.checker;
using System.Net;
using Microsoft.AspNetCore.Authorization;
using Monitoring.Models.MonitoringModule;

namespace Monitoring.Controllers;

[ApiController]
[Route("api/[controller]")]
[AllowAnonymous]
public class MonitoringController : ControllerBase
{
    private readonly MonitoringDbContext _context;
    private readonly MonitoringSystem _monitoringSystem;

    public MonitoringController(
        MonitoringDbContext context,
        MonitoringSystem monitoringSystem)
    {
        _context = context;
        _monitoringSystem = monitoringSystem;
    }

    [HttpGet("websites")]
    public async Task<ActionResult<IEnumerable<Website>>> GetWebsites()
    {
        Console.WriteLine("GetWebsites");
        try
        {
            var websites = await _context.Websites
                .Include(w => w.CheckResults)
                .AsNoTracking()
                .ToListAsync();
            
            return Ok(websites);
        }
        catch (Exception ex)
        {
            return Problem(
                detail: $"Error retrieving websites: {ex.Message}",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    [HttpPost("check/{id}")]
    public async Task<ActionResult<Website>>RunCheck(int id)
    {
        var queryParams = HttpContext.Request.Query;
        Console.WriteLine(queryParams);
        try
        {
            Console.WriteLine("1");
            var website = await _context.Websites
                .FirstOrDefaultAsync(w => w.Id == id);
            Console.WriteLine("2");

            if (website == null)
            {
                return NotFound($"Website with ID {id} not found");
            }
            Console.WriteLine("3");

            _monitoringSystem.addUrl(website,
                int.Parse(queryParams["interval"].ToString()),
                queryParams["checkerClass"].ToString(),
                queryParams["content"].ToString(),
                int.Parse(queryParams["retries"].ToString()));
            Console.WriteLine("4");

            return website;
        }
        catch (ArgumentException ex)
        {
            return BadRequest(ex.Message);
        }
        catch (Exception ex)
        {
            return Problem(
                detail: $"Error performing check: {ex.Message}",
                statusCode: StatusCodes.Status500InternalServerError);
        }
        
    }

    // GET: api/monitoring/results/{websiteId}
    [HttpGet("results/{websiteId}")]
    public async Task<ActionResult<IEnumerable<CheckResult>>> GetCheckResults(int websiteId)
    {
        try
        {
            var results = await _context.CheckResults
                .Where(cr => cr.websiteId == websiteId)
                .OrderByDescending(cr => cr.Timestamp)
                .AsNoTracking()
                .ToListAsync();

            return Ok(results);
        }
        catch (Exception ex)
        {
            return Problem(
                detail: $"Error retrieving results: {ex.Message}",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    // GET: api/monitoring/status/{websiteId}
    [HttpGet("status/{websiteId}")]
    public async Task<IActionResult> GetCurrentStatus(int websiteId)
    {
        try
        {
            var latestResult = await _context.CheckResults
                .Where(cr => cr.websiteId == websiteId)
                .OrderByDescending(cr => cr.Timestamp)
                .FirstOrDefaultAsync();

            if (latestResult == null)
            {
                return NotFound("No check results available");
            }

            return Ok(new {
                Status = latestResult.isUp ? "Healthy" : "Unhealthy",
                LastChecked = latestResult.Timestamp,
                ResponseTime = latestResult.responseTime,
                Error = latestResult.error
            });
        }
        catch (Exception ex)
        {
            return Problem(
                detail: $"Error retrieving status: {ex.Message}",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }
}