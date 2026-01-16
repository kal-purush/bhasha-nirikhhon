using FoodImportLabelGenerator.Data;
using FoodImportLabelGenerator.Models;
using Microsoft.EntityFrameworkCore;

namespace FoodImportLabelGenerator.Repository;

public class UserRepository : IUserRepository
{
    private readonly IConfiguration _configuration;
    private readonly DbContextOptions<UsersContext> _options;
    
    public UserRepository(DbContextOptions<UsersContext> options, IConfiguration configuration)
    {
        _configuration = configuration;
        _options = options;
    }
    
    public IEnumerable<User> GetAll()
    {
        using var dbContext = new UsersContext(_options, _configuration);
        return dbContext.Users.ToList();
    }

    public IEnumerable<User> GetByUserName(string userName)
    {
        using var dbContext = new UsersContext(_options, _configuration);
        return dbContext.Users.Where(u => u.UserName!.ToLower().Contains(userName.ToLower())).ToList();
    }

    public User? GetByEmail(string email)
    {
        using var dbContext = new UsersContext(_options, _configuration);
        return dbContext.Users.FirstOrDefault(u=>u.Email == email);
    }
    
    public User? GetById(string id)
    {
        using var dbContext = new UsersContext(_options, _configuration);
        return dbContext.Users.FirstOrDefault(u=>u.Id == id);
    }
    
    public IEnumerable<User> GetByCompanyName(string companyName)
    {
        using var dbContext = new UsersContext(_options, _configuration);
        return dbContext.Users.Where(u=>u.CompanyName!.ToLower().Contains(companyName.ToLower())).ToList();
    }

    public void Add(User user)
    {
        using var dbContext = new UsersContext(_options, _configuration);
        dbContext.Add(user);
        dbContext.SaveChanges();
    }

    public void Update(User updatedUser)
    {
        using var dbContext = new UsersContext(_options, _configuration);
        dbContext.Update(updatedUser);
        dbContext.SaveChanges();
    }

    public void Delete(User user)
    {
        using var dbContext = new UsersContext(_options, _configuration);
        dbContext.Remove(user);
        dbContext.SaveChanges();
    }
}