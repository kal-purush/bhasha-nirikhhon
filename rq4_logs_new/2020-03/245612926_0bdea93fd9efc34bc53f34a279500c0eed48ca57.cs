using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using CourseGenerator.BLL.Infrastructure;
using CourseGenerator.BLL.DTO;
using CourseGenerator.Models.Entities.Identity;

namespace CourseGenerator.BLL.Interfaces
{
    public interface IUserManagementService : IDisposable
    {
        Task<OperationInfo> CreateAsync(UserRegistrationDTO registrationDto);
        Task<OperationInfo> ExistsWithUserNameAsync(string username);
        Task<OperationInfo> AddToRoleAsync(User user, string role);
    }
}