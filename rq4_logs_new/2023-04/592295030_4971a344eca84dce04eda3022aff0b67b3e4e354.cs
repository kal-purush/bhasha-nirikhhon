using Core.Abstractions.Repositories;
using Core.Abstractions.Services;
using Domain;
using FluentValidation;
using Models.Validators;
using Models.ViewModels;
using Services.Exceptions;

namespace Services
{
    public class CategoryService : ICategoryService
    {
        private readonly ICategoryRepository _repository;
        private readonly IValidator<CategoryViewModel> _viewModelValidator;

        public CategoryService(
            ICategoryRepository repository,
            IValidator<CategoryViewModel> viewModelValidator)
        {
            _repository = repository;
            _viewModelValidator = viewModelValidator;
        }

        public CategoryViewModel GetById(long categoryId)
        {
            var foundCategory = MapToViewModel(_repository.GetById(categoryId));

            if (foundCategory == null)
                throw new ResourceNotFoundException($"Category with id: {categoryId} was not found.");

            return foundCategory;
        }

        public List<CategoryViewModel> GetAllCategories()
        {
            return _repository
                .GetAllCategories()
                .Select<Category, CategoryViewModel>(p => MapToViewModel(p))
                .Where(p => p != null)
                .ToList();
        }

        public void Insert(CategoryViewModel categoryViewModel)
        {
            _viewModelValidator.ValidateAndThrow(categoryViewModel);

            Category Category = MapFromViewModel(categoryViewModel);

            if (Category == null)
                throw new ArgumentNullException(nameof(categoryViewModel));

            _repository.Insert(Category);
        }

        public bool Update(long categoryId, CategoryViewModel categoryViewModel)
        {
            Category Category = MapFromViewModel(categoryViewModel);

            if (Category == null)
                throw new ArgumentNullException(nameof(categoryViewModel));

            return _repository.Update(categoryId, Category);
        }

        public bool Delete(long categoryId)
        {
            bool isDeleted = _repository.Delete(categoryId);
            if (!isDeleted)
            {
                throw new ResourceNotFoundException($"Category with id: {categoryId} was not found!");
            }
            return true;
        }

        public List<CategoryViewModel> SearchByKeyWord(string keyword)
        {
            return _repository
                .GetAllCategories()
                .Where(c => c.Name.Contains(keyword, StringComparison.InvariantCultureIgnoreCase))
                .Select<Category, CategoryViewModel>(p => MapToViewModel(p))
                .ToList();
        }

        private CategoryViewModel MapToViewModel(Category p)
        {
            if (p == null)
                return null;

            return new CategoryViewModel
            {
                Id = p.Id,
                Name = p.Name,
            };
        }

        private Category MapFromViewModel(CategoryViewModel p)
        {
            if (p == null)
                return null;

            return new Category
            {
                Id = p.Id,
                Name = p.Name,
            };
        }

    }
}