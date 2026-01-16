using System.Collections.Generic;

namespace StockportGovUK.NetStandard.Gateways.Models.Verint.VerintOnlineForm;

public class VerintOnlineFormUpdateRequest
{
    public string VerintOnlineFormReference { get; set; }
    public IDictionary<string, string> FormData { get; set; }
}