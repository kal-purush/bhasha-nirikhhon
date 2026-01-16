using System;
using System.Collections.Generic;
using System.Linq;
using Sistema_Ferreteria.Models;

namespace Sistema_Ferreteria.Controllers
{
    internal class CategoryController
    {
        private static List<Category> categories = new List<Category>();

        // Constructor
        public CategoryController()
        {
            
        }

        // agregar
        public void AddCategory(string name)
        {
            int newId = categories.Count > 0 ? categories.Max(c => c.Id) + 1 : 1;
            var newCategory = new Category(newId, name);
            categories.Add(newCategory);
        }

        //actualizar una categoría
        public bool UpdateCategory(int id, string newName)
        {
            var category = categories.FirstOrDefault(c => c.Id == id);
            if (category != null)
            {
                category.Update(newName);
                return true;
            }
            return false;
        }

        //obtener todas las categorías
        public List<Category> GetCategories()
        {
            return categories;
        }

        //obtener una categoría por ID
        public Category GetCategoryById(int id)
        {
            return categories.FirstOrDefault(c => c.Id == id);
        }

        //eliminar una categoría
        public bool DeleteCategory(int id)
        {
            var category = categories.FirstOrDefault(c => c.Id == id);
            if (category != null)
            {
                categories.Remove(category);
                return true;
            }
            return false;
        }
    }
}