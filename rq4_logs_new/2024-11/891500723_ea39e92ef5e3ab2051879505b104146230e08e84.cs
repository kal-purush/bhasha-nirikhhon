using HomeAccounting.Application.Integrations.Commands;
using HomeAccounting.Application.Integrations.Contracts;
using Microsoft.AspNetCore.Mvc;
using Swashbuckle.AspNetCore.Annotations;

namespace HomeAccounting.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CurrencyExchangeController : ControllerBase
    {
        private readonly ICurrencyExchangeService _currencyExchangeService;

        public CurrencyExchangeController(ICurrencyExchangeService currencyExchangeService)
        {
            _currencyExchangeService = currencyExchangeService;
        }

        
        [HttpPost("exchange/pair")]
        [SwaggerOperation(
            Summary = "Exchanges a pair of currencies",
            Description = "Exchanges a pair of currencies based on the provided request.",
            OperationId = "ExchangePair",
            Tags = new[] { "CurrencyExchange" }
        )]
        [SwaggerResponse(200, "Returns the exchange response.", typeof(ExchangePairResultResponse))]
        [SwaggerResponse(400, "The currency response is invalid.")]
        [SwaggerResponse(500, "Internal server error")]
        public async Task<IActionResult> ExchangePairAsync([FromQuery] ExchangeRequest request)
        {
            try
            {
                var exchangeResponse = await _currencyExchangeService.ExchangePairAsync(request);
                return Ok(exchangeResponse);
            }
            catch (HttpRequestException ex)
            {
                return StatusCode(500, $"{ex.Message}");
            }
            catch (KeyNotFoundException ex)
            {
                return BadRequest("Invalid currency code.");
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"{ex.Message}");
            }
        }

        
        [HttpPost("exchange/rates/{currency}")]
        [SwaggerOperation(
            Summary = "Gets the latest exchange rates for a specific currency",
            Description = "Gets the latest exchange rates for the specified currency.",
            OperationId = "ExchangeLatest",
            Tags = new[] { "CurrencyExchange" }
        )]
        [SwaggerResponse(200, "Returns the exchange rates response.", typeof(ExchangeRatesResultResponse))]
        [SwaggerResponse(400, "The currency response is invalid.")]
        [SwaggerResponse(500, "Internal server error")]
        public async Task<IActionResult> ExchangeRatesAsync(string currency)
        {
            try
            {
                var exchangeResponse = await _currencyExchangeService.ExchangeRatesAsync(currency);
                return Ok(exchangeResponse);
            }
            catch (HttpRequestException ex)
            {
                return StatusCode(500, $"{ex.Message}");
            }
            catch (KeyNotFoundException ex)
            {
                return BadRequest("Invalid currency code.");
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"{ex.Message}");
            }
        }
    }
}