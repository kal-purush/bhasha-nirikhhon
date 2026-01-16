using Microsoft.AspNet.Identity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Wunderlist.Web.Models;
using System.Threading.Tasks;

namespace Wunderlist.Web.Infrastructure
{
    public class CustomRoleStore : IRoleStore<OwinRole>
    {
        public Task CreateAsync(OwinRole role)
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync(OwinRole role)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public Task<OwinRole> FindByIdAsync(string roleId)
        {
            throw new NotImplementedException();
        }

        public Task<OwinRole> FindByNameAsync(string roleName)
        {
            throw new NotImplementedException();
        }

        public Task UpdateAsync(OwinRole role)
        {
            throw new NotImplementedException();
        }
    }
}