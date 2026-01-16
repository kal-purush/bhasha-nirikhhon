using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BulkInsertNET.Model;
using System.Data.SqlClient;

namespace BulkInsertNet.Repository.BulkInserter
{
    public class CategorySimpleSqlBulkCopyRepository : ICategoryRepository
    {
        private string categoriesTableName = "Categories";

        private readonly string connectionString;
        public CategorySimpleSqlBulkCopyRepository(string connectionString)
        {
            this.connectionString = connectionString;
        }
        public void BulkInsert(List<Category> categories)
        {
            using (var ssbc = new SimpleSqlBulkCopy(connectionString))
            {
                ssbc.WriteToServer(categoriesTableName, categories);
            }
        }
    }
}