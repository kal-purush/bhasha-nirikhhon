using TestBuildersPattern.ClassLibrary.Entities;

namespace UnitTests.TestBuilders;
public sealed class ClientBuilder
{
    private readonly long _id = 123;
    private string _name = "test";
    private int _age = 123;
    private string _author = "joao";
    private string _description = "asas";

    public static ClientBuilder NewObject() =>
        new();

    public Client DomainBuild() =>
        new()
        {
            Name = _name,
            Age = _age,
            Author = _author,
            Description = _description,
            Id = _id
        };

    public ClientBuilder WithName(string name)
    {
        _name = name;

        return this;
    }

    public ClientBuilder WithAge(int age)
    {
        _age = age;

        return this;
    }

    public ClientBuilder WithAuthor(string author)
    {
        _author = author;

        return this;
    }

    public ClientBuilder WithDescription(string description)
    {
        _description = description;

        return this;
    }
}