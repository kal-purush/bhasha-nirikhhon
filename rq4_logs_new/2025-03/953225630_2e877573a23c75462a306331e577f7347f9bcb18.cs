using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using AsxDbScraper.Services;
using AsxDbScraper.Models;

namespace AsxDbScraper.Functions;

public class AsxCompaniesFunction
{
    private readonly IAsxCompanyService _companyService;
    private readonly ILogger<AsxCompaniesFunction> _logger;

    public AsxCompaniesFunction(
        IAsxCompanyService companyService,
        ILogger<AsxCompaniesFunction> logger)
    {
        _companyService = companyService;
        _logger = logger;
    }

    [FunctionName("UpdateCompaniesList")]
    public async Task<IActionResult> UpdateCompaniesListAsync(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req)
    {
        try
        {
            _logger.LogInformation("Triggering ASX companies list update");
            await _companyService.UpdateCompaniesListAsync();
            return new OkObjectResult(new { message = "Companies list updated successfully" });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating companies list");
            return new StatusCodeResult(500);
        }
    }

    [FunctionName("GetAllCompanies")]
    public async Task<IActionResult> GetAllCompaniesAsync(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req)
    {
        try
        {
            var companies = await _companyService.GetAllCompaniesAsync();
            return new OkObjectResult(companies);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching companies list");
            return new StatusCodeResult(500);
        }
    }

    [FunctionName("GetCompanyByCode")]
    public async Task<IActionResult> GetCompanyByCodeAsync(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "companies/{code}")] HttpRequest req,
        string code)
    {
        try
        {
            var company = await _companyService.GetCompanyByCodeAsync(code);
            if (company == null)
            {
                return new NotFoundObjectResult(new { message = $"Company with code {code} not found" });
            }
            return new OkObjectResult(company);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching company with code {Code}", code);
            return new StatusCodeResult(500);
        }
    }
} 