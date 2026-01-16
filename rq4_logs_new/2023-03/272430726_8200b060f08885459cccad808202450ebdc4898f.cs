using SFA.DAS.DfESignIn.Auth.Interfaces;

namespace SFA.DAS.Tools.Support.Web.Infrastructure
{
    /// <summary>
    /// Class to define the Custom Service Role used in DfESignIn Authentication Service.
    /// </summary>
    public class CustomServiceRole : ICustomServiceRole
    {
        public string RoleClaimType => "http://service/service";
    }
}