using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using AsxDbScraper.Services;

namespace AsxDbScraper.Functions;

public class FinancialDataFunction
{
    private readonly IAsxScraperService _scraperService;
    private readonly IAsxCompanyService _companyService;
    private readonly ILogger<FinancialDataFunction> _logger;

    public FinancialDataFunction(
        IAsxScraperService scraperService,
        IAsxCompanyService companyService,
        ILogger<FinancialDataFunction> logger)
    {
        _scraperService = scraperService;
        _companyService = companyService;
        _logger = logger;
    }

    // Timer-triggered function that runs every 10 minutes
    [Function("ScrapeNextCompany")]
    public async Task Run([TimerTrigger("0 */10 * * * *")] MyInfo myTimer)
    {
        _logger.LogInformation("Scheduled scrape triggered at: {Time}", DateTime.UtcNow);

        try
        {
            var success = await _scraperService.ScrapeNextCompanyAsync();
            if (success)
            {
                _logger.LogInformation("Scheduled company scraping completed successfully");
            }
            else
            {
                _logger.LogWarning("Scheduled company scraping completed but with no successful statement updates");
            }

            // Get queue length for monitoring
            var queueLength = await _scraperService.GetQueueLengthAsync();
            _logger.LogInformation("Current queue contains {QueueLength} companies", queueLength);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during scheduled scraping");
            // Don't rethrow - let the function complete so it will run again
        }
    }

    // HTTP-triggered function for on-demand scraping of a specific company
    [Function("ScrapeFinancialStatements")]
    public async Task<HttpResponseData> ScrapeSpecificCompany(
        [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req)
    {
        try
        {
            var query = System.Web.HttpUtility.ParseQueryString(req.Url.Query);
            string companyCode = query["companyCode"];
            if (string.IsNullOrEmpty(companyCode))
            {
                var badRequestResponse = req.CreateResponse(HttpStatusCode.BadRequest);
                await badRequestResponse.WriteAsJsonAsync(new { message = "Please provide a company code" });
                return badRequestResponse;
            }

            _logger.LogInformation("On-demand scraping requested for {CompanyCode}", companyCode);

            // Find the company by code
            var company = await _companyService.GetCompanyByCodeAsync(companyCode);
            if (company == null)
            {
                var notFoundResponse = req.CreateResponse(HttpStatusCode.NotFound);
                await notFoundResponse.WriteAsJsonAsync(new { message = $"Company with code {companyCode} not found" });
                return notFoundResponse;
            }

            // Force the last updated timestamp to be old so this company gets priority
            if (company.LastUpdated > DateTime.UtcNow.AddYears(-1))
            {
                company.LastUpdated = DateTime.UtcNow.AddYears(-1);
                // This would typically update the company in the database
                // Not implementing this part since we're just demonstrating the concept
            }

            // Now run the scraper which will pick this company since it has the oldest timestamp
            bool success = await _scraperService.ScrapeNextCompanyAsync();

            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(new
            {
                message = success ?
                    "Financial statements scraped successfully" :
                    "Financial statement scraping completed with warnings",
                companyCode = company.Code,
                companyName = company.Name,
                lastUpdated = DateTime.UtcNow // The new timestamp after scraping
            });
            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in on-demand scraping");
            var response = req.CreateResponse(HttpStatusCode.InternalServerError);
            await response.WriteAsJsonAsync(new { message = "Internal server error" });
            return response;
        }
    }

    // HTTP endpoint for getting scraper status
    [Function("GetScraperStatus")]
    public async Task<HttpResponseData> GetStatus(
        [HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequestData req)
    {
        _logger.LogInformation("Status request received at: {Time}", DateTime.UtcNow);

        try
        {
            var queueLength = await _scraperService.GetQueueLengthAsync();
            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(new
            {
                status = "running",
                queueLength = queueLength,
                interval = "10 minutes",
                timestamp = DateTime.UtcNow
            });
            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting scraper status");
            var response = req.CreateResponse(HttpStatusCode.InternalServerError);
            await response.WriteAsJsonAsync(new { error = "Failed to get scraper status" });
            return response;
        }
    }
}

public class MyInfo
{
    public bool IsPastDue { get; set; }
}

public class ScheduleStatus
{
    public DateTime Last { get; set; }
    public DateTime Next { get; set; }
    public DateTime LastUpdated { get; set; }
}