using NerdStore.Core.DomainObjects;
using NerdStore.Core.DomainObjects.Interfaces;

namespace NerdStore.Catalog.Domain;

public class Product : Entity, IAggregateRoot
{
    public Guid CategoryId { get; private set; }
    public string Name { get; private set; }
    public string Description { get; private set; }
    public bool Active { get; private set; }
    public decimal Value { get; private set; }
    public DateTime RegistrationDate { get; private set; }
    public string Image { get; private set; }
    public int QuantityInStock { get; private set; }

    public Category Category { get; private set; }

    public Product(
        Guid categoryId,
        string name,
        string description,
        bool active,
        decimal value,
        DateTime registrationDate,
        string image)
    {
        CategoryId = categoryId;
        Name = name;
        Description = description;
        Active = active;
        Value = value;
        RegistrationDate = registrationDate;
        Image = image;
    }

    public void Activate() => Active = true;

    public void Deactivate() => Active = false;

    public void ChangeDescription(string description) => Description = description;

    public void ReplenishStock(int quantity) => QuantityInStock += quantity;

    public bool HasStock(int quantity) => QuantityInStock >= quantity;

    public void DebitStock(int quantity)
    {
        if (quantity < 0) quantity *= -1;
        QuantityInStock -= quantity;
    }

    public void ChangeCategory(Category category)
    {
        Category = category;
        CategoryId = category.Id;
    }

    public void Validate()
    {
    }
}