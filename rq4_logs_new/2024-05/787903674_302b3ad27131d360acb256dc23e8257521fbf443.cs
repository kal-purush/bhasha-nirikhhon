using CoreService.Application.Models;
using Microsoft.IdentityModel.Tokens;
using System;
using System.Collections.Generic;
using System.Data;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;

namespace CoreService.Application.DTOs
{
    public sealed class AdminSignInDto : BaseTokenGenerator
    {
        public AdminSignInDto()
        {
            Id = string.Empty;
            Username = string.Empty;
            Email = string.Empty;
            Firstname = null;
            Lastname = null;
            CompanyName = null;
            RoleName = null;
            RoleLevel = null;
            Screens = new HashSet<string>();
        }

        private AdminSignInDto(string id, string username, string email, string? firstname, string? lastname, string? companyName, string? roleName, UInt16? roleLevel)
        {
            Id = id;
            Username = username;
            Email = email;
            Firstname = firstname;
            Lastname = lastname;
            CompanyName = companyName;
            RoleName = roleName;
            RoleLevel = roleLevel;
            Screens = new HashSet<string>();
        }

        public string Id { get; private set; }
        public string? Firstname { get; private set; }
        public string? Lastname { get; private set; }
        public string? CompanyName { get; private set; }
        public ICollection<string> Screens { get; set; }

        public static AdminSignInDto CreateSignInDto(string id, string username, string email, string? firstname, string? lastname, string? companyName, string? roleName, UInt16? roleLevel)
        {
            return new AdminSignInDto(id, username, email, firstname, lastname, companyName, roleName, roleLevel);
        }

        public void AddScreensRange(ICollection<string> screens)
        {
            if (screens.Count == 0) return;
            foreach (var currentScreen in screens)
            {
                Screens.Add(currentScreen);
            }
        }
    }
}