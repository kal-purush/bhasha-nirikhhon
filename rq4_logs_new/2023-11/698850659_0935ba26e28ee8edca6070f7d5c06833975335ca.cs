using Microsoft.VisualBasic.ApplicationServices;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MusicManager.Repsitory
{
    public interface IRepoUser
    {
        public Task<IEnumerable<Model.User>> GetUsers();
        public Task<bool> PatchUser(Model.User user);
        public Task<IEnumerable<Model.Role>> GetRoles();
    }
}