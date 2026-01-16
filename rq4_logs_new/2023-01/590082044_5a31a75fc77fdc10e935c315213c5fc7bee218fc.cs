using CraftHouse.Web.Data;
using CraftHouse.Web.Entities;
using Microsoft.AspNetCore.Mvc;

namespace CraftHouse.Web.Repositories;

public class UserRepository : IUserRepository
{
   private readonly AppDbContext _context;

   public UserRepository(AppDbContext context)
   {
      _context = context;
   }
   
   public User? GetUserById(int id)
   {
      var user = _context.Users.FirstOrDefault(x => x.Id == id);
      return user;
   }

   public User? GetUserByEmail(string email)
   {
      var user = _context.Users.FirstOrDefault(x => x.Email == email);
      return user;
   }
}