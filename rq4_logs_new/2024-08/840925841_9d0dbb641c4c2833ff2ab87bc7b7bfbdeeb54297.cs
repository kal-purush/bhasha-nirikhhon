namespace PetFamily.Domain.Entities.ValueObjects;

public record FullName
{
    private FullName(string firstName, string surname, string patronymic)
    {
        FirstName = firstName;
        Surname = surname;
        Patronymic = patronymic;
    }
    
    public string FirstName { get; }

    public string Surname { get; }

    public string Patronymic { get; }

    public static FullName Create(string firstName, string surname, string patronymic)
    {
        var fullName = new FullName(firstName, surname, patronymic);
        
        return fullName;
    }
}