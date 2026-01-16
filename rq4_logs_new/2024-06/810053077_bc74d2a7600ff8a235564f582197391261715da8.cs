using backend_carhelp.Iam.Domain.Model.Entities;
using backend_carhelp.Workshop_management.Domain.Model.Commands;

namespace backend_carhelp.Workshop_management.Domain.Model.Aggregates;

public class Product
{
    public int Id { get; set; }
    public string Name { get; set; }
    public int Quantity{ get; set; }
    public double Price{ get; set; }
    public string ImageUrl{ get; set; }
    public int WorkshopId{ get; set; }

    public Product()
    {
        Name= string.Empty;
        Quantity = 0;
        Price = 0;
        ImageUrl = string.Empty;
        WorkshopId = 0;
    }
    
    public Product(string name, int quantity, double price, string imageUrl, int workshopId) {
        Name = name;
        Quantity = quantity;
        Price = price;
        ImageUrl = imageUrl;
        WorkshopId = workshopId;
    }

    public Product(CreateProductCommand createProductCommand)
    {
        Name = createProductCommand.Name;
        Quantity = createProductCommand.Quantity;
        Price = createProductCommand.Price;
        ImageUrl = createProductCommand.ImageUrl;
        WorkshopId = createProductCommand.WorkshopId;
    }
}