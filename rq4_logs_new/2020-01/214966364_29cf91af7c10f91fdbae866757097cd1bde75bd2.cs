using DataLayer;
using DataLayer.SqlConnection;
using System;
using System.Collections.Generic;

namespace ServiceLayer
{
    public class DataService
    {
        private readonly IDataRepository dataRepository;

        public DataService(IDataRepository dataRepository)
        {
            this.dataRepository = dataRepository;
        }

        public List<ProductCategory> GetAllProductCategories()
        {
            return dataRepository.GetAllProductCategories();
        }

        public void AddProductCategory(string name)
        {
            dataRepository.AddProductCategory(new ProductCategory
            {
                Name = name,
                ModifiedDate = DateTime.Today
            });
        }

        public void UpdateProductCategory(string name, int id)
        {
            dataRepository.UpdateProductCategory(name, id);
        }

        public void DeleteProductCategory(int id)
        {
            dataRepository.DeleteProductCategory(id);
        }

    }
}