using Microsoft.AspNetCore.Authorization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hippologamus.DTO 
{
    public static  class Policies
    {
        public const string CanViewErrorLogs = "CanViewErrorLogs";

        public static AuthorizationPolicy CanViewErrorLogsPolicy()
        {
            return new AuthorizationPolicyBuilder()
                .RequireAuthenticatedUser()
                .RequireClaim("country", "BE")
                //.RequireRole() This is for doing roles
                .Build();
        }
    }
}