using System.ComponentModel.DataAnnotations;

namespace CraftHouse.Web.Entities;

public class Product
{
    [Key]
    public int Id { get; set; }

    [Required]
    public string Name { get; set; } = null!;

    [Required]
    public bool IsAvailable { get; set; }
    
    [Required]
    public float Price { get; set; }

    
    /*
    [Required]
    public string MeatType { get; set; } = null!;
    // TODO CHANGE MEAT TYPE
    
    [Required]
    public string Sauce { get; set; } = null!;
    // TODO CHANGE SAUCE TYPE
            
    [Required]
    public int Weight { get; set; }
    
    [Required]
    public string Size { get; set; } = null!;
    // TODO CHANGE SIZE TYPE
    
    public List<string> Addons { get; set; } = new();
    // TODO CHANGE ADDONS TYPE
        
    public List<string> Ingredients { get; set; } = new();
    // TODO CHANGE INGREDIENTS TYPE
    */
}