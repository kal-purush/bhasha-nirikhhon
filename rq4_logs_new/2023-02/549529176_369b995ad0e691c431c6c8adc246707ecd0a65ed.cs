using Answer.King.Domain;
using Answer.King.Domain.Repositories.Models;

namespace Answer.King.Infrastructure.UnitTests.TestObjects;

public class WrongConstructor
{
    private readonly HashSet<TagId> tags;

    public WrongConstructor(string name, string description, double price, ProductCategory category)
    {
        Guard.AgainstNullOrEmptyArgument(nameof(name), name);
        Guard.AgainstNullOrEmptyArgument(nameof(description), description);
        Guard.AgainstNegativeValue(nameof(price), price);

        this.Id = 0;
        this.Name = name;
        this.Description = description;
        this.Price = price;
        this.Category = category;
        this.tags = new HashSet<TagId>();
    }

    // ReSharper disable once UnusedMember.Local
#pragma warning disable IDE0051 // Remove unused private members
    private WrongConstructor(
        long id,
        string name,
        string description,
        double price,
        ProductCategory category,
        IList<TagId> tags,
        bool retired)
    {
#pragma warning restore IDE0051 // Remove unused private members
        Guard.AgainstDefaultValue(nameof(id), id);
        Guard.AgainstNullOrEmptyArgument(nameof(name), name);
        Guard.AgainstNullOrEmptyArgument(nameof(description), description);
        Guard.AgainstNegativeValue(nameof(price), price);
        Guard.AgainstNullArgument(nameof(category), category);
        Guard.AgainstNullArgument(nameof(tags), tags);

        this.Id = id;
        this.Name = name;
        this.Description = description;
        this.Price = price;
        this.Category = category;
        this.tags = new HashSet<TagId>(tags);
        this.Retired = retired;
    }

    public long Id { get; set; }

    public string Name { get; set; }

    public string Description { get; set; }

    public double Price { get; set; }

    public ProductCategory Category { get; private set; }

    public IReadOnlyCollection<TagId> Tags => this.tags;

    public bool Retired { get; private set; }
}