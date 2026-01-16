namespace VanKassa.Domain.ViewModels;

public class EmployeeViewModel
{
    public string LastName { get; init; }
    public string FirstName { get; init; }
    public string Patronymic { get; init; }
    public string Addresses { get; init; }
    public string Photo { get; init; }

    public EmployeeViewModel(string lastName, string firstName, string patronymic, string addresses, string photo)
    {
        LastName = lastName;
        FirstName = firstName;
        Patronymic = patronymic;
        Addresses = addresses;
        Photo = photo;
    }
}