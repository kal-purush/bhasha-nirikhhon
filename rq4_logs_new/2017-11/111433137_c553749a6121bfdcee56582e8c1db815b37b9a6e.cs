using System;
using RestSharp;
using Retrofit.Net.Attributes.Methods;
using Retrofit.Net.Attributes.Parameters;

namespace exercise1
{
    public interface IHelper
    {
        [Post("lists")]
        RestResponse<bool> CreateList([Body] Object obj);

        [Post("lists/{idList}/members")]
        RestResponse<bool> AddEmail([Path("idList")] string id, [Body] object obj);

        [Post("campaigns")]
        RestResponse<bool> CreateCampaigns([Body] Object obj);

        [Put("campaigns/{idCompaign}/content")]
        RestResponse<bool> SetContent([Path("idCompaign")] string campaignId, [Body] object obj);

        [Post("campaigns/{idCompaign}/actions/send")]
        RestResponse<bool> SendCampaign([Path("idCompaign")] string campaignId);
    }
}