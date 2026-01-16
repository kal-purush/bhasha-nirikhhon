using PsicopataPedidos.Domain.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PsicopataPedidos.Application
{
    public class UserService : IUserService
    {
        private readonly IUserRepository _userRepository;
        public UserService(IUserRepository userRepository)
        {
            _userRepository = userRepository;
        }

        public User CreateUser(User user)
        {
            _userRepository.CreateUser(user);
            return user;
        }

       

        public User DeleteUser(User user)
        {
            _userRepository.DeleteUser(user);
            return user;
        }

        public List<User> GetAllUsers(User user)
        {
            var users = _userRepository.GetAllUsers();
            return users;
        }

        public List<User> GetAllUsers()
        {
            var users = _userRepository.GetAllUsers();

            return users;
        }
    }
}