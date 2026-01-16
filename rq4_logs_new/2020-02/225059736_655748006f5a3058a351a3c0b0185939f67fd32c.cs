using AJS.Services.Interfaces;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Localization;
using System.Threading.Tasks;

namespace AJS.Services.Implementations
{
    public class ServiceHelper : IServiceHelper
    {
        private readonly IHttpContextAccessor httpAccessor;

        public ServiceHelper(IHttpContextAccessor httpAccessor)
        {
            this.httpAccessor = httpAccessor;
        }

        public async Task<HttpContext> GetCurrentHttpContext()
        {
            return await Task.Run(() => httpAccessor.HttpContext);
        }

        public async Task<string> GetUserLocalization()
        {
            var httpContext = await GetCurrentHttpContext();

            return  httpContext.Features
                                     .Get<IRequestCultureFeature>()
                                     .RequestCulture.Culture
                                     .Name
                                     .ToString();
        }
    }
}