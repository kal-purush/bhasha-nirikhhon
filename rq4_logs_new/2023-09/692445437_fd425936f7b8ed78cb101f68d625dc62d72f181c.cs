using ISM.Application.Interfaces;
using ISM.Domain.Models;
using ISM.Infrastructure.Services;

namespace ISM.Infrastructure.Checking
{
    public class CheckService : IChecks
    {
        private readonly ServiceforUsers _users;
        private readonly ServiceForOrders _orders;
        private readonly ServiceforRoles _roles;
        public CheckService()
        {
            _users = new();
            _orders = new();
            _roles = new();
        }

        public User Password(string Email, string password)
        {

            var users = _users.GetAll();
            var ourUser = users.FirstOrDefault(x => x.Email == Email && x.Password == password);
            if (ourUser != null)
                return ourUser;
            return null;
        }
    }
}