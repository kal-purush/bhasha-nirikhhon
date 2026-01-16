using BussinessLayer.Interface;
using DataBaseLayer.Users;
using RepositoryLayer.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace BussinessLayer.Services
{
    public class AdminBL : IAdminBL
    {
        IAdminRL adminRl;

        public AdminBL(IAdminRL adminRl)
        {
            this.adminRl = adminRl;
        }

        public void AddAdmin(AdminPostModel adminPostModel)
        {
            try
            {
                this.adminRl.AddAdmin(adminPostModel);
            }
            catch (Exception)
            {

                throw;
            }
        }

        public string LoginAdmin(string Email, string Password)
        {
            try
            {
                return this.adminRl.LoginAdmin(Email, Password);
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
}