using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FinalProject.Domain.Model;

namespace FinalProject.Data.Interfaces
{
    public interface ICategoryRepository
    {
        IList<Category> GetCategory();
        Category GetCategoryById(int categoryId);
        int InsertCategory(Category categoty);
        int DeleteCategory(int categoryId);
        int UpdateCategory(Category category);
        void Save();
    }
}