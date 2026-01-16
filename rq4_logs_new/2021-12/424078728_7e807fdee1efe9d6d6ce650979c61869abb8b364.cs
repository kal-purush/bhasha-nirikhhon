using Microsoft.AspNetCore.Identity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Gestao_de_Projetos.Data
{
    public class RolesConfig
    {
        private const string ROLE_GESTOR = "Gestor";
        private const string ROLE_MEMBRO = "Membro";
        private const string ROLE_CLIENTE = "Cliente";
        
        private static async Task CheckRoles(RoleManager<IdentityRole> roleManager, string role)
        {   

            if (!await roleManager.RoleExistsAsync(role))
            {
                await roleManager.CreateAsync(new IdentityRole(role));
            }
        }

        public static async Task CreateRolesAndUsers(UserManager<IdentityUser> userManager, RoleManager<IdentityRole> roleManager)
        {
            const string GESTOR_USER = "admin@noemail.com";
            const string GESTOR_PASSWORD = "sECRET$123";
            const string MEMBRO = "Lua@gmail.com";
            const string MEMBRO_PASSWORD = "sECREDO$123";
            const string CLIENTE = "pedro12@gmail.com";
            const string CLIENTE_PASSWORD = "sECREDO$123";


            CheckRoles(roleManager, ROLE_GESTOR).Wait();
            CheckRoles(roleManager, ROLE_MEMBRO).Wait();
            CheckRoles(roleManager, ROLE_CLIENTE).Wait();


            IdentityUser gestor = await userManager.FindByNameAsync(GESTOR_USER);
            if (gestor == null)
            {
                gestor = new IdentityUser { UserName = GESTOR_USER };
                await userManager.CreateAsync(gestor, GESTOR_PASSWORD); //cria user
            }

            IdentityUser membro = await userManager.FindByNameAsync(MEMBRO);
            if (membro == null)
            {
                membro = new IdentityUser { UserName = MEMBRO };
                await userManager.CreateAsync(membro, MEMBRO_PASSWORD);
            }

            IdentityUser cliente = await userManager.FindByNameAsync(CLIENTE);

            if (cliente == null)
            {
                cliente = new IdentityUser { UserName = CLIENTE };
                await userManager.CreateAsync(cliente, CLIENTE_PASSWORD);
            }

            if (!await userManager.IsInRoleAsync(gestor, ROLE_GESTOR))
            {
                await userManager.AddToRoleAsync(gestor, ROLE_GESTOR);
            }

            if (!await userManager.IsInRoleAsync(membro, ROLE_MEMBRO))
            {
                await userManager.AddToRoleAsync(membro, ROLE_MEMBRO);
            }

            if (!await userManager.IsInRoleAsync(cliente, ROLE_CLIENTE))
            {
                await userManager.AddToRoleAsync(cliente, ROLE_CLIENTE);
            }
        }



        
    }

}
