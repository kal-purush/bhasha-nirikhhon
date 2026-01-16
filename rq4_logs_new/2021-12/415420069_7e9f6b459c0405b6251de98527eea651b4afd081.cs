using DAL.DTO;
using DAL.Interfaces;
using System;
using System.Collections.Generic;

namespace WikiFSUnitTests.Mockups
{
    public class SQLUserContextMockup : IUserContainer
    {
        public List<UserDTO> userList = new List<UserDTO>();
        public SQLUserContextMockup()
        {
            userList.Add(new UserDTO
            {
                ID = 1,
                UserName = "Administrator",
                Email = "someone@example.com",
                Password = "nonH@shedPassword!",
                created_at = DateTime.Now,
            });
        }

        public void CreateUser(UserDTO user)
        {
            userList.Add(user);
            return;
        }
    }
}