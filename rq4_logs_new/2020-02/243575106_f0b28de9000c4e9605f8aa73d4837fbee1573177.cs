using System.Collections.Generic;
using System.Threading.Tasks;
using file.Core;
using file.Core.Models;
using file.Core.Services;

namespace file.Services
{
    public class UserService : IUserService
    {
        private readonly IUnitOfWork _unitOfwork;

        public UserService(IUnitOfWork unitOfWork)
        {
            _unitOfwork = unitOfWork;
        }

        public async Task<User> CreateUser(User user)
        {
            // Insert the new entity and commit the change to repositories and sent data to DB
            // return the inserted entity
            await _unitOfwork.Users.AddAsync(user);
            await _unitOfwork.Commit();
            return user;
        }

        public async Task DeleteUser(User user)
        {
            // Removes the entity from DB and commit changes
            _unitOfwork.Users.Remove(user);
            await _unitOfwork.Commit();
        }

        public async Task<IEnumerable<User>> GetAllUsers()
        {
            // Return all the users
            return await _unitOfwork.Users.GetAllAsync();
        }

        public async Task<User> GetUserById(int id)
        {
            // Return a user by id
            return await _unitOfwork.Users.GetByIdAsync(id);
        }

        public async Task<User> GetUserWithFilesById(int id)
        {
            return await _unitOfwork.Users.GetUserWithFileById(id);
        }

        public async Task UpdateUser(User userToUpdate, User user)
        {
            // Updates the entity and commits changes to DB
            userToUpdate.name = user.name;
            userToUpdate.lastname = user.lastname;
            await _unitOfwork.Commit();
        }
    }
}