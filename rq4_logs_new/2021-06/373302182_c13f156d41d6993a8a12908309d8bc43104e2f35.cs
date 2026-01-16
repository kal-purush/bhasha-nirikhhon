using System;
using System.Collections.Generic;
using System.Linq;
using NeutrackAPI.Models;

namespace NeutrackAPI.DTOs
{
    public class AuthResponseDTO
    {
        public int Id { get; set; }
        public string Email { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string FullName { get; set; }
        public string Token { get; set; }
        public List<string> Roles { get; set; }
        public AuthResponseDTO(User user, string token)
        {
            Id = user.Id;
            Email = user.Email ;
            FirstName = user.FirstName;
            LastName = user.LastName;
            FullName  = user.FullName;
            Token = token;
            Roles = user.UserRoles.Select(x => x.Role.Name).ToList();
        }
    }
}