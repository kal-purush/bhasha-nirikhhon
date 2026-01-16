using Application.IServices;
using Data.Models;
using Data.ViewModels;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.Services
{
    public class UserService : IUserService
    {
        private readonly DrugRepositoryDBContext _context;
        public UserService(DrugRepositoryDBContext context)
        {
            _context = context;
        }
        public async Task<InfoUserView> getInfoUser(string request)
        {
            InfoUserView result = new InfoUserView()
            {
                UserId = "",
                Email = "",
                FullName = "",
                PhoneNumber = ""
            };
            if (request == null)
            {
                return result;
            }
            try
            {
                var res = await _context.Users.FromSqlRaw($"USP_INFO_USER {request}").ToListAsync();
                var userT = res.FirstOrDefault();
                if (userT != null)
                {
                    result.UserId = userT.UserId;
                    result.Email = userT.Email;
                    result.FullName = userT.FullName;
                    result.PhoneNumber = userT.PhoneNumber;
                    return result;
                }
                else
                {
                    return result;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }


        }
    }
}