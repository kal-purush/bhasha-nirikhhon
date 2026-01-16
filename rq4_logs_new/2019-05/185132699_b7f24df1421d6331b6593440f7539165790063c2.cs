using Dao.Utils;
using Model;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dao
{
    public class CategoryDao
    {
        public List<Category> queryAllCategory()
        {
            String sql = "SELECT * FROM tb_category";
            SqlCommand cmd = DbUtil.getCommand(sql);
            SqlDataReader sdr = cmd.ExecuteReader();

            List<Category> list = new List<Category>();
            if (sdr.HasRows)
            {
                while (sdr.Read())
                {
                    Category category = new Category();
                    category.Id = sdr.GetInt32(0);
                    category.CategoryName = sdr.GetString(1);
                    category.CategoryDesc = sdr.GetString(2);
                    //category.CategoryImg = sdr.GetString(3);
                    category.Priority = sdr.GetInt32(4);
                    //category.ParentId = sdr.GetInt32(5);
                   // category.CreateTime = sdr.GetDateTime(6);
                   // category.ModifyTime = sdr.GetDateTime(7);
                    list.Add(category);
                }
            }

            sdr.Close();
            DbUtil.close();
            return list;
        }
    }
}