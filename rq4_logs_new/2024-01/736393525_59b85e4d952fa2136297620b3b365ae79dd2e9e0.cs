using BusinessLogic.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BusinessLogic.Interfaces
{
    public abstract class ICrud<TModel> where TModel : class
    {
        public abstract Task<IEnumerable<TModel>> GetAllAsync(PaginationModel paginationModel);
        public abstract Task<TModel> GetByIdAsync(int id);
        public abstract Task<TModel> AddAsync(TModel model);
        public abstract Task<TModel> UpdateAsync(TModel model);
        public abstract Task<TModel> DeleteAsync(int id);
        protected IEnumerable<TModel> ApplyPagination(IEnumerable<TModel> models, int pageNumber, int pageSize)
        {
            return models.Skip((pageNumber - 1) * pageSize).Take(pageSize);
        }
    }

}