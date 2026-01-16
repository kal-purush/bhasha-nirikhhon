using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DTO
{
    public record UserDTO(string Email,string Password, string? FirstName, string? LastName);
    public record UserDTOGet(int Id, string Email, string? FirstName, string? LastName);
}