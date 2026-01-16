using System.Collections.Generic;
using System.Threading.Tasks;

namespace Billing.API.Services.Invoice
{
    public class DummyInvoiceProviderService : IInvoiceService
    {
        public async Task<IEnumerable<string>> GetInvoices(string clientId) =>
            await Task.Run(() => new List<string>
            {
                "File1.pdf",
                "File2.pdf"
            });

        public Task TestSapConnection()
        {
            throw new System.NotImplementedException();
        }
    }
}