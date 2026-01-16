using EntityDatabaseFirst.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EntityDatabaseFirst.Function
{
    public class GetData
    {
         public List<TblPost> GetAllPost()

        {
            var db = new BlogContext();

            return db.TblPosts.ToList();
            
        }
        public List<TblPost> GetIdPost(int ID)

        {
            var db = new BlogContext();

            return db.TblPosts.Where(m => m.Id == ID).ToList();

        }
    }
}