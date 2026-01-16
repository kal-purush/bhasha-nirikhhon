using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Identity;

namespace CosmeticsStore.Repositories.Models.Domain;

public class ApplicationUser : IdentityUser
{
    [MaxLength(45)]
    public string? FirstName { get; set; }
    [MaxLength(45)]
    public string? LastName { get; set; }
    [MaxLength(200)]
    public string? Address { get; set; }
    public int Gender { get; set; }
    public DateTimeOffset BirthDate { get; set; }
    public string? PhoneNumber { get; set; }
    public bool Status { get; set; }
}