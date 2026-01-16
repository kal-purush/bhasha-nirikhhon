using System.ComponentModel.DataAnnotations;

namespace CraftHouse.Web.Entities;

public class User : EntityBase
{
    [Key]
    public int Id { get; set; }

    [Required]
    public string FirstName { get; set; } = null!;

    [Required]
    public string LastName { get; set; } = null!;

    [Required]
    public int TelephoneNumber { get; set; }

    [Required]
    public string City { get; set; } = null!;

    [Required]
    public string PostalCode { get; set; } = null!;

    [Required]
    public string AddressLine { get; set; } = null!;
}