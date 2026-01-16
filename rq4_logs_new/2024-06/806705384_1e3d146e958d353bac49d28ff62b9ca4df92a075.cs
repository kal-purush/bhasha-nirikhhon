using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DAL
{
    public class CategoryDAL
    {
        private static CategoryDAL instance;

        public static CategoryDAL Instance
        {
            get
            {
                if (instance == null)
                {
                    instance = new CategoryDAL();
                }
                return CategoryDAL.instance;
            }
            private set { CategoryDAL.instance = value; }
        }
        public CategoryDAL() { }
        public DataTable GetCategories()
        {
            string query = "SELECT CategoryName, CategoryID FROM Category";
            return Database.Instance.ExecuteQuery(query);
        }
    }
}