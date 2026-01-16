using LoginApi.Datas;
using LoginApi.IRepositories;
using LoginApi.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace LoginApi.Repositories
{
    public class CompanyRepository : ICompanyRepository
    {
        public readonly UserDbContext _userDbContext;
        public CompanyRepository(UserDbContext dbContext)
        {
            _userDbContext = dbContext;
        }
        public List<UserLoginClass> getuser()
        {
            return _userDbContext.Logincred.ToList();
        }


        public async Task<UserLoginClass> authentication([FromBody]UserLoginClass checkuser)
        {
            try
            {
                var auth = _userDbContext.Logincred.FirstOrDefault(x => x.Username == checkuser.Username && x.Password == checkuser.Password);
                if (auth != null)
                {
                    return auth;
                }
                else
                {
                    throw new Exception("User not found");
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
     
        }



        public async Task<bool> deleteuser(string username)
        {
            var del = await this._userDbContext.Logincred.FirstOrDefaultAsync(x => x.Username == username);
            if(del == null)
            {
                return false;
            }
            _userDbContext.Logincred.Remove(del);
            return true;
        }



        public async Task createuser(UserLoginClass newuser)
        {
           this._userDbContext.Logincred.Add(newuser);
            await this._userDbContext.SaveChangesAsync();
        }
    }
}