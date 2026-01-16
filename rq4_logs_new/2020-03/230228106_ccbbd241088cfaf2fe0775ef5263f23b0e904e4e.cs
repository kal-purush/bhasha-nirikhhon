using iTechArt.SchoolSchedule.DomainModel.Users;
using Microsoft.AspNet.Identity;
using System.Security.Claims;
using System.Threading.Tasks;

namespace iTechArt.SchoolSchedule.Foundation.Interfaces
{
    public interface ISchoolScheduleUserManager
    {
        Task<SchoolScheduleUser> FindAsync(string userName, string password);

        Task<ClaimsIdentity> CreateIdentityAsync(SchoolScheduleUser user, string authenticationType);

        Task<IdentityResult> CreateAsync(SchoolScheduleUser user, string password);
    }
}