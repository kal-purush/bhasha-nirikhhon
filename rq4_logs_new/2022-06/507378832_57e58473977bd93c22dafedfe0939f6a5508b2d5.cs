using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Http.Cors;
using System.Web.Http;
using System.Web.Cors;
using System.Threading.Tasks;
using System.Net.Http;
using System.Threading;

namespace CitasAPI.App_Start
{
    public class AccessPolicyCors : Attribute, ICorsPolicyProvider
    {
        public async Task<CorsPolicy> GetCorsPolicyAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var corsRequestContext = request.GetCorsRequestContext();
            var originRequested = corsRequestContext.Origin;
            if (await IsOriginFromCustomer(originRequested))
            {
                var polocy = new CorsPolicy
                {
                    AllowAnyHeader = true,
                    AllowAnyMethod = true
                };

                polocy.Origins.Add(originRequested);
                return polocy;
            }
            return null;

        }

        private async Task<bool> IsOriginFromCustomer(string originRequested)
        {
            return true;
        }
    }
}