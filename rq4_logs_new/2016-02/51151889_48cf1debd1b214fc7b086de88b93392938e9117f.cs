namespace BoardGames.Services.Data
{
    using System;
    using System.Linq;
    using BoardGames.Data.Models;
    using BoardGames.Services.Data.Contracts;
    using BoardGames.Data.Common;

    public class CategoriesService : ICategoriesService
    {
        private readonly IDbRepository<Category> categories;

        public CategoriesService(IDbRepository<Category> categories)
        {
            this.categories = categories;
        }

        public void Add(string name)
        {
            var newCategory = new Category
            {
                Name = name
            };

            this.categories.Add(newCategory);
            this.categories.Save();
        }

        public void Delete(int id)
        {
            var categoryToBeDeleted = this.categories.GetById(id);
            this.categories.Delete(categoryToBeDeleted);
            this.categories.Save();
        }

        public void Edit(int id, string name)
        {
            var categoryToBeEdited = this.categories.GetById(id);
            categoryToBeEdited.Name = name;
            this.categories.Save();
        }

        public IQueryable<Category> GetAll()
        {
            return this.categories.All();
        }

        public Category GetById(int id)
        {
            return this.categories.GetById(id);
        }
    }
}