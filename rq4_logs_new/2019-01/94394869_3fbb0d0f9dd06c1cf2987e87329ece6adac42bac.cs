using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using YanSoft.CurrencyExchanger.Core.Configurations;
using YanSoft.CurrencyExchanger.Core.Models.Dto;
using YanSoft.CurrencyExchanger.Core.Utils;

namespace YanSoft.CurrencyExchanger.Core.Services
{
    public class ApiService : IApiService
    {
        private readonly IHttpClientService httpClientService;
        public ApiService(IHttpClientService httpClientService)
        {
            this.httpClientService = httpClientService;
        }

        public async Task<ResponseInfo<CurrencyRatesResponse>> GetLatestRates(string sourceCode, string targetCodes)
        {
            var url = $"{AppConfigurations.ApiUrl}GetLatestRates?sourceCode={sourceCode}&targetCodes={targetCodes}";
            var result = new ResponseInfo<CurrencyRatesResponse>();
            try
            {
                var response = await httpClientService.CreateClient().GetAsync(url);
                if (response.IsSuccessStatusCode)
                {
                    var responseString = await response.Content.ReadAsStringAsync();
                    result = SerializerHelper.FromJson<ResponseInfo<CurrencyRatesResponse>>(responseString);
                }
                return result;
            }
            catch
            {
                result.Message = "Service unavailable.";
                return result;
            }
        }
    }
}