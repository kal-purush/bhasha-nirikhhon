using System;
using HRM.ApplicationCore.Model.Request;
using HRM.ApplicationCore.Model.Response;

namespace HRM.ApplicationCore.Contract.Service
{
	public interface IServiceAsync<TRequestModel, TResponseModel>
        where TRequestModel : class
        where TResponseModel : class
	{
        Task<int> AddAsync(TRequestModel model);
        Task<int> DeleteAsync(int id);
        Task<int> UpdateAsync(TRequestModel model);
        Task<TResponseModel> GetByIdAsync(int id);
        Task<IEnumerable<TResponseModel>> GetAllAsync();
    }
}
