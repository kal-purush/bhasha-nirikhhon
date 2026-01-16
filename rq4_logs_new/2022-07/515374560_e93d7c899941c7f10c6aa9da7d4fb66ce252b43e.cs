using CloudCustomers.API.Models;

public interface IUsersService
{
    Task<List<User>> GetAllUsers();
}
