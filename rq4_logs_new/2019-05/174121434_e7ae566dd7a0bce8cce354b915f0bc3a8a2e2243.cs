using HtmlAgilityPack;
using Microsoft.ApplicationInsights.AspNetCore;
using Microsoft.AspNetCore.Http;
using NWebsec.AspNetCore.Core.Web;
using NWebsec.Mvc.Common.Helpers;

namespace DfE.EmployerFavourites.Web.Helpers
{
    public class ApplicationInsightsJsHelper
    {
        private readonly JavaScriptSnippet _aiJavaScriptSnippet;
        private readonly IHttpContextAccessor _httpContextAccessor;

        public ApplicationInsightsJsHelper(IHttpContextAccessor accessor, JavaScriptSnippet aiJavaScriptSnippet)
        {
            _aiJavaScriptSnippet = aiJavaScriptSnippet;
            _httpContextAccessor = accessor;
        }
        public string Script
        {
            get
            {
                var js = _aiJavaScriptSnippet.FullScript;

                if (string.IsNullOrWhiteSpace(js))
                    return string.Empty;

                var httpContext = new HttpContextWrapper(_httpContextAccessor.HttpContext);
                var helper = new CspConfigurationOverrideHelper();
                var nonce = helper.GetCspScriptNonce(httpContext);

                var htmlDoc = new HtmlDocument();
                htmlDoc.LoadHtml(js);

                var scriptNode = htmlDoc.DocumentNode.SelectSingleNode("//script");

                scriptNode.Attributes.Append("nonce");

                scriptNode.SetAttributeValue("nonce", nonce);

                return scriptNode.OuterHtml;
            }
        }
    }
}