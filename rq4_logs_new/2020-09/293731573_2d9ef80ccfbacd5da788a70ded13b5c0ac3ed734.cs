using Dal.AbstractInterfaces;
using Entities.Models;
using Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SiparisTakip.Bll
{
    public class CategoryManager : GenericManager<Categories>, ICategoriesService
    {
        ICategoriesRepository categoriesRepository;

        public CategoryManager(ICategoriesRepository categoriesRepository) : base(categoriesRepository)
        {
            this.categoriesRepository = categoriesRepository;
        }

        public List<Categories> GetCategories()
        {
            return categoriesRepository.GetCategories();
        }
    }
}