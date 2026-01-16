using System;
using System.Collections.Generic;
using System.Linq;
using EShop.Entity;

namespace EShop.ServiceLayer
{
    public class EShopDataService : IDataService
    {
        private static readonly Exception NullProviderError;
        static EShopDataService()
        {
            NullProviderError = new Exception("EShopDataService: data provider not initialised.");
        }

        private readonly IModelRepository _modelRepository;
        private readonly ICategoryRepository _categoryRepository;

        //конструктор службы бизнес-логики использует в качестве параметров реализации интерфейсов репозиториев
        public EShopDataService(IModelRepository modelrepository, ICategoryRepository categoryRepository)
        {
            _modelRepository = modelrepository;
            _categoryRepository = categoryRepository;
        }

        public void Dispose()
        {
            if (_modelRepository != null)
                _modelRepository.Dispose();
            if (_categoryRepository != null)
                _categoryRepository.Dispose();
        }
        //метод IDataService.GetRandomModels - пример совместного использования репозиториев
        public IEnumerable<Model> GetRandomModels(int categoryId, int number)
        {
            if (_modelRepository == null) throw NullProviderError;
            if (_categoryRepository == null) throw NullProviderError;
            var modelsCount = _categoryRepository.CountModels(categoryId);
            var rnd = new Random();
            var modelList = _modelRepository.GetModels(categoryId).ToArray();
            var randomModels = new List<Model>();
            var index = 0;
            while (index < number)
            {
                var n = rnd.Next(0, modelsCount);
                var model = modelList[n];
                if (model == null) continue;
                if (randomModels.Any(m => m.Id == model.Id)) continue;
                randomModels.Add(modelList[n]);
                index++;
            }
            return randomModels;
        }
        //---------------------------------------------------------------
        //пример собственной логики приложения - обход дерева категорий и составления 
        //линейного списка всех категорий 
        public IEnumerable<Category> GetCategoryList()
        {
            var list = new List<Category>();
            foreach (var c in RootCategories)
            {
                list.Add(c);
                list.AddRange(GetAllSubCategories(c.Id));
            }
            return list.OrderBy(c => c.Id).Select(c => c);
        }
        //------------------------------------------------------------------------------
        //далее бизнес-логика будет простая и на вид просто прокси класс для передачи вызова в репозитории
        //но это в нашем простом приложении. В сложном промышленном приложении все было бы намного сложнее
        public bool NewCategory(Category item)
        {
            if (_categoryRepository == null) throw NullProviderError;
            return _categoryRepository.AddCategory(item);
        }

        public bool DeleteCategory(int itemId)
        {
            if (_categoryRepository == null) throw NullProviderError;
            return _categoryRepository.DeleteCategory(itemId);
        }

        public Category GetCategory(int categoryId)
        {
            if (_categoryRepository == null) throw NullProviderError;
            return _categoryRepository.GetCategory(categoryId);
        }

        public IEnumerable<Category> RootCategories
        {
            get
            {
                if (_categoryRepository == null) throw NullProviderError;
                return _categoryRepository.RootCategories;
            }
        }

        public bool DeleteModel(int modelId)
        {
            if (_modelRepository == null) throw NullProviderError;
            return _modelRepository.DeleteModel(modelId);
        }

        public Model GetModel(int modelId)
        {
            if (_modelRepository == null) throw NullProviderError;
            return _modelRepository.GetModel(modelId);
        }

        public bool ChangeCategory(Category item)
        {
            if (_categoryRepository == null) throw NullProviderError;
            return _categoryRepository.UpdateCategory(item);
        }

        public IEnumerable<Category> PathToRoot(int categoryId)
        {
            if (_categoryRepository == null) throw NullProviderError;
            return _categoryRepository.PathToRoot(categoryId);
        }

        public IEnumerable<Category> GetSubCategories(int categoryId)
        {
            if (_categoryRepository == null) throw NullProviderError;
            return _categoryRepository.GetSubCategories(categoryId);
        }

        public IEnumerable<Category> GetAllSubCategories(int categoryId)
        {
            if (_categoryRepository == null) throw NullProviderError;
            return _categoryRepository.GetAllSubCategories(categoryId);
        }

        public int ModelsInCategory(int categoryId)
        {
            if (_categoryRepository == null) throw NullProviderError;
            return _categoryRepository.CountModels(categoryId);
        }

        public bool NewModel(Model item)
        {
            if (_modelRepository == null) throw NullProviderError;
            return _modelRepository.AddModel(item);
        }

        public bool ChangeModel(Model item)
        {
            if (_modelRepository == null) throw NullProviderError;
            return _modelRepository.UpdateModel(item);
        }

        public IEnumerable<Model> GetModels(int categoryId)
        {
            if (_modelRepository == null) throw NullProviderError;
            return _modelRepository.GetModels(categoryId);
        }

        public IEnumerable<Model> GetModels(int categoryId, int from, int pageSize)
        {
            if (_modelRepository == null) throw NullProviderError;
            return _modelRepository.GetModels(categoryId, from, pageSize);
        }
    }
}
