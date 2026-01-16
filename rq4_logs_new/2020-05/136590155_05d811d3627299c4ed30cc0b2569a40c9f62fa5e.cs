using Com.Danliris.Service.Sales.Lib.Models.SalesInvoice;
using Com.Danliris.Service.Sales.Lib.Services;
using Com.Danliris.Service.Sales.Lib.ViewModels.SalesInvoice;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Com.Danliris.Sales.Test
{
    public class SalesInvoiceHttpClientTestService : IHttpClientService
    {
        public static string Token;
        public SalesInvoiceModel _model;
        public SalesInvoiceHttpClientTestService(SalesInvoiceModel model)
        {
            _model = model;
        }

        public Task<HttpResponseMessage> PutAsync(string url, HttpContent content)
        {
            return Task.Run(() => new HttpResponseMessage()
            {
                StatusCode = System.Net.HttpStatusCode.OK
            });
        }
        public Task<HttpResponseMessage> GetAsync(string url)
        {
            return Task.Run(() => new HttpResponseMessage()
            {
                StatusCode = System.Net.HttpStatusCode.OK,
                Content = new StringContent("{data:{'data':'data', 'Buyers':{'Id':1}, 'Address' :'ad', 'BankAddress':'ad', 'SwiftCode':'ad', 'BankName':'ad', 'Currency' : {'Id':1}}}")
            });
        }

        public Task<HttpResponseMessage> PostAsync(string url, HttpContent content)
        {
            SalesInvoiceReportSalesReceiptViewModel data = new SalesInvoiceReportSalesReceiptViewModel()
            {
                CurrencySymbol = "IDR",
                Nominal = 10,
                SalesInvoiceId = _model.Id,
                SalesReceiptDate = DateTimeOffset.UtcNow,
                SalesReceiptNo = "np",
                TotalPaid = 10,
                UnPaid = 0,
            };

            string dataJson = JsonConvert.SerializeObject(new List<SalesInvoiceReportSalesReceiptViewModel>() { data });
            return Task.Run(() => new HttpResponseMessage()
            {
                StatusCode = System.Net.HttpStatusCode.Created,
                Content = new StringContent(dataJson)
            });
        }
    }
}