using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace Persistence.Entity;

[Index(nameof(Name), IsUnique = true)]
internal class ProductInfo
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; init; }

    [Column(TypeName = "VARCHAR")]
    [StringLength(50)]
    [Required]
    public required string Name { get; init; }
    
    [Column(TypeName = "VARCHAR")]
    [StringLength(1000)]
    [Required]
    public required string Description { get; init; }
    
    [Column(TypeName = "VARCHAR")]
    [StringLength(50)]
    [Required]
    public required string Category { get; init; }

    [Column(TypeName = "VARCHAR")]
    [StringLength(150)]
    [Required]
    public string? Link { get; init; }
    
    [DeleteBehavior(DeleteBehavior.Restrict)]
    public IList<OrderProduct> OrderProducts { get; init; } = new List<OrderProduct>();
}