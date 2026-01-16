using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using projeto_integrado_2_sem.Models;

namespace projeto_integrado_2_sem.Repositories
{
    class RegisterUser
    {
        private User oneUser;
        
        public void setOneUser(User u)
        {
            this.oneUser = u;
            registerUser();
        }

        private void registerUser()
        {
            RepositoryManager.ManagerInstance.userRepository.persist(new User(null, oneUser.email, oneUser.password, Profile.UserProfile()));
        }
    }
}