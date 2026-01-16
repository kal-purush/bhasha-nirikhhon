using System.Security.Cryptography.X509Certificates;
using System.Security.Principal;
using Scrumfish.WebApi.v2.Objects.Interfaces;

namespace Scrumfish.WebApi.v2.Filters.Tests.Mock
{
    public class AuthenticateMock : ICertificateAuthenticationProvider
    {
        public bool ValidateReturnValue { get; set; }
        public IPrincipal GetPrincipalReturnValue { get; set; }

        public IPrincipal GetUnsecuredUserForTestingOnlyReturnValue { get; set; }

        public bool Validate(X509Certificate2 certificate)
        {
            return ValidateReturnValue;
        }

        public IPrincipal GetPrincipal(X509Certificate2 certificate)
        {
            return GetPrincipalReturnValue;
        }

        public IPrincipal GetUnsecuredUserForTestingOnly()
        {
            return GetUnsecuredUserForTestingOnlyReturnValue;
        }
    }
}