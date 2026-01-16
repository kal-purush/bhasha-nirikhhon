using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Api.Extensions
{
    public static class HttpContextExtensions
    {
        public static List<Guid> GetTenantIds(this HttpContext context)
        {
            var claims = context.User.Claims;
            var tenants = claims.Where(x => x.Type == "tenant").ToList();

            if (tenants == null || tenants.Count <= 0)
            {
                tenants = claims.Where(x => x.Type == "client_tenant").ToList();
            }

            var tenantIds = new List<Guid>();

            foreach (var tenantId in tenants)
            {
                Guid.TryParse(tenantId.Value, out Guid thisTenantId);

                if (thisTenantId != null)
                {
                    tenantIds.Add(thisTenantId);
                }
            }

            return tenantIds;
        }
    }
}