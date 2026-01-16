using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Sustainsys.Saml2.AspNetCore2;

namespace SampleAspNetCore2ApplicationNETFramework
{
    public class SimpleOptions : AuthenticationSchemeOptions
    {
        public string DisplayMessage { get; set; }
        public Saml2Options options { get; set; }
    }

    public class SimpleAuthHandler : AuthenticationHandler<SimpleOptions>
    {
        public SimpleAuthHandler(IOptionsMonitor<SimpleOptions> options, ILoggerFactory logger, UrlEncoder encoder, ISystemClock clock) : base(options, logger, encoder, clock)
        { }

        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            throw new NotImplementedException();
        }
    }
}