using Microsoft.Extensions.Logging;
using SPSS.Entities;
using SPSS.Repository.UnitOfWork;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPSS.Service.Services.CategoryService
{
    public class CategoryService(IUnitOfWork _unitOfWork, ILogger<CategoryService> _logger) : ICategoryService
    {
        public async Task AddAsync(Category c)
        {
            try
            {
                _logger.LogInformation("Adding a new category: {CategoryName}", c.CategoryName);
                await _unitOfWork.Categories.AddAsync(c);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error adding category: {CategoryName}", c.CategoryName);
                throw;
            }
        }

        public async Task DeleteAsync(int id)
        {
            try
            {
                _logger.LogInformation("Deleting category ID {Id}", id);
                var category = await _unitOfWork.Categories.GetByIdAsync(id);
                if (category == null)
                {
                    _logger.LogWarning("Category with ID {Id} not found.", id);
                    throw new KeyNotFoundException($"Category with ID {id} not found.");

                }
                else await _unitOfWork.Categories.DeleteAsync(id);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error deleting category ID {Id}", id);
                throw;
            }
        }

        public async Task<IEnumerable<Category>> GetAllAsync()
        {
            try
            {
                _logger.LogInformation("Fetching all categories.");
                return await _unitOfWork.Categories.GetAllAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error fetching all category.");
                throw new Exception("An error occurred while retrieving cateogry.", ex);
            }
        }

        public async Task<Category> GetByIdAsync(int id)
        {
            try
            {
                _logger.LogInformation("Fetching category with ID {Id}", id);
                var category = await _unitOfWork.Categories.GetByIdAsync(id);

                if (category == null)
                {
                    _logger.LogWarning("Category with ID {Id} not found.", id);
                    throw new KeyNotFoundException($"Category with ID {id} not found.");
                }

                return category;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error fetching category with ID {Id}", id);
                throw;
            }
        }

        public async Task UpdateAsync(int id, Category c)
        {
            try
            {
                _logger.LogInformation("Updating category ID {Id}", c.Id);
                var category = await _unitOfWork.Categories.GetByIdAsync(id);
                if (category == null)
                {
                    _logger.LogWarning("Category with ID {Id} not found.", id);
                    throw new KeyNotFoundException($"Category with ID {id} not found.");
                }
                await _unitOfWork.Categories.UpdateAsync(c);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating category ID {Id}", c.Id);
                throw;
            }
        }
    }
}