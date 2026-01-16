namespace Blog.Repositories.Interfaces;

public interface IUserRepository
{
    bool UserExists(string email);
}