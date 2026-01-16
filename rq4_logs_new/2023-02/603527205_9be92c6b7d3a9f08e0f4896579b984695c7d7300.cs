using System;
using HRM.ApplicationCore.Contract.Repository;
using HRM.ApplicationCore.Contract.Service;
using HRM.ApplicationCore.Entity;
using HRM.ApplicationCore.Model.Request;
using HRM.ApplicationCore.Model.Response;
using HRM.Infrastructure.Repository;

namespace HRM.Infrastructure.Service
{
	public class EmployeeRoleServiceAsync : IEmployeeRoleServiceAsync
    {
        private readonly IEmployeeRoleRepositoryAsync employeeRoleRepositoryAsync;

        public EmployeeRoleServiceAsync(IEmployeeRoleRepositoryAsync _employeeRoleRepositoryAsync)
		{
            employeeRoleRepositoryAsync = _employeeRoleRepositoryAsync;
        }

        public Task<int> AddAsync(EmployeeRoleRequestModel model)
        {
            EmployeeRole employeeRole = new EmployeeRole()
            {
                Title = model.Title,
                description = model.description,
                IsActive = model.IsActive
            };
            return employeeRoleRepositoryAsync.InsertAsync(employeeRole);
        }

        public Task<int> DeleteAsync(int id)
        {
            return employeeRoleRepositoryAsync.DeleteAsync(id);
        }

        public async Task<IEnumerable<EmployeeRoleResponseModel>> GetAllAsync()
        {
            var result = await employeeRoleRepositoryAsync.GetAllAsync();
            if (result != null)
            {
                return result.ToList().Select(x => new EmployeeRoleResponseModel()
                {
                    Id = x.Id,
                    Title = x.Title,
                    description = x.description,
                    IsActive = x.IsActive,
                });
            }
            return null;
        }

        public async Task<EmployeeRoleResponseModel> GetByIdAsync(int id)
        {
            var result = await employeeRoleRepositoryAsync.GetByIdAsync(id);
            if (result != null)
            {
                return new EmployeeRoleResponseModel()
                {
                    Id = result.Id,
                    Title = result.Title,
                    description = result.description,
                    IsActive = result.IsActive,
                };
            }
            return null;
        }

        public async Task<int> UpdateAsync(EmployeeRoleRequestModel model)
        {
            EmployeeRole employeeRole = new EmployeeRole()
            {
                Id = model.Id,
                Title = model.Title,
                description = model.description,
                IsActive = model.IsActive
            };
            return await employeeRoleRepositoryAsync.UpdateAsync(employeeRole);
        }
    }
}
