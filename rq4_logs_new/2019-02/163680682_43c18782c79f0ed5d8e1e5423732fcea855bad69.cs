using System.Threading.Tasks;
using Microsoft.AspNetCore.DataProtection;
using SFA.DAS.SecureMessageService.Core.IRepositories;

namespace SFA.DAS.SecureMessageService.Infrastructure.Repositories
{
    public class DasDataProtector : IDasDataProtector
    {
        private readonly IDataProtector dataProtector;

        public DasDataProtector(IDataProtectionProvider _provider)
        {
            dataProtector = _provider.CreateProtector("SFA.DAS.SecureMessageService.Infrastructure.Repositories.DasDataProtector");
        }

        public string Protect(string message)
        {
            return dataProtector.Protect(message);
        }
        
        public string Unprotect(string message)
        {
             return dataProtector.Unprotect(message);
        }
    }
}