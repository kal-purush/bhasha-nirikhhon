using BusinessLayer.Interface;
using DatabaseLayer;
using DataBaseLayer;
using ReposatoryLayer.Interface;
using System;
using System.Collections.Generic;
using System.Text;

namespace BusinessLayer.Services
{
    public class UserBL : IUserBL
    {
        IUserRL userRL;
        public UserBL(IUserRL userRL)
        {
            this.userRL = userRL;
        }
        public void AddUser(UsersModel users)
        {
            try
            {
                this.userRL.AddUser(users);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        


    }
}