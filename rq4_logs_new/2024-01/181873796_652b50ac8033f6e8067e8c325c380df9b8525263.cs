using System.Net.Http;
using System.Threading.Tasks;
using StockportGovUK.NetStandard.Gateways.Models.Whitespace.Request;
using StockportGovUK.NetStandard.Gateways.Response;

namespace StockportGovUK.NetStandard.Gateways.WasteOrchestrationService;

public class WasteOrchestrationServiceGateway : Gateway, IWasteOrchestrationServiceGateway
{
    private const string HomeEndpoint = "api/v1/Home";

    public WasteOrchestrationServiceGateway(HttpClient httpClient) : base(httpClient)
    {
    }

    public async Task<HttpResponse<string>> ProcessWorksheetRequest(CreateWorksheetRequest request)
        => await PostAsync<string>($"{HomeEndpoint}", request);
}