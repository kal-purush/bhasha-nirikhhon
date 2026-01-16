using Auction.LMS.CORE.Data;
using Auction.LMS.CORE.Repoisitory;
using Auction.LMS.CORE.Services;
using System;
using System.Collections.Generic;
using System.Text;

namespace Auction.LMS.INFRA.Services
{
   public class CategoryService: ICategoryService
    {
        private readonly ICategoryRepository categoryRepository;
        public CategoryService(ICategoryRepository categoryRepository)
        {
            this.categoryRepository = categoryRepository;
        }

        public bool DeleteCategory(int id)
        {
            return categoryRepository.DeleteCategory(id);
        }

        public List<Category> GetAllCategory()
        {
            return categoryRepository.GetAllCategory();
        }

        public bool InsertCategory(Category category)
        {
            return categoryRepository.InsertCategory(category);
        }

        public List<Category> SearchByCategoryName(Category category)
        {
        return categoryRepository.SearchByCategoryName(category)
        }

        public bool UpdateCategory(Category category)
        {
            return categoryRepository.UpdateCategory(category);
        }
    }
}