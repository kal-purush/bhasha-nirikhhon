using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using RestaurauntApp.DTOS;

namespace RestaurauntApp.Repositories
{
    public class MenuRepository : IMenuRepository
    {
        private readonly SqlConnection connection;
        public MenuRepository(string connectionString)
        {
            connection = new SqlConnection(connectionString);
        }
        public Task<int> CreateMenuItemAsync(MenuItemDTO newMenuItem)
        {
            throw new NotImplementedException();
        }

        public Task<int> DeleteMenuItemAsync(int id)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<MenuItem>> GetAllMenuItemsAsync()
        {
            throw new NotImplementedException();
        }

        public Task<MenuItem> GetMenuItemByIdAsync(int id)
        {
            throw new NotImplementedException();
        }

        public Task<int> UpdateMenuItemAsync(int id, MenuItemDTO menuItemToUpdate)
        {
            throw new NotImplementedException();
        }
    }
}