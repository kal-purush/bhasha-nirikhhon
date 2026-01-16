using CoreService.Application.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CoreService.Application.DTOs
{
    public sealed class RefreshMyTokenDto : BaseTokenGenerator
    {
        public RefreshMyTokenDto(string username, string email, string? roleName, UInt16? roleLevel)
        {
            Username = username;
            Email = email;
            RoleName = roleName;
            RoleLevel = roleLevel;
        }

        public static RefreshMyTokenDto CreateRefreshMyToken(string username, string email, string? roleName, UInt16? roleLevel)
        {
            return new RefreshMyTokenDto(username, email, roleName, roleLevel);
        }
    }
}