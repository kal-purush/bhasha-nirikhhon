using Database.Databases;
using Domain.Models;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.Users.Commands.CreateUser
{
    internal class AddNewUserCommandHandler : IRequestHandler<AddNewUserCommand, User>
    {
        private readonly FakeDatabase _fakeDatabase;

        public AddNewUserCommandHandler(FakeDatabase fakeDatabase)
        {
            _fakeDatabase = fakeDatabase;
        }

        public Task<User> Handle(AddNewUserCommand request, CancellationToken cancellationToken)
        {
            try
            {                
                User userToCreate = new User
                {
                    Id = Guid.NewGuid(),
                    UserName = request.UserDto.UserName,
                    Password = request.UserDto.Password
                };
                _fakeDatabase.Users.Add(userToCreate);
                return Task.FromResult(userToCreate);
            }
            catch
            {
                throw new Exception("User not added");
            }
        }
            
        
    }
}