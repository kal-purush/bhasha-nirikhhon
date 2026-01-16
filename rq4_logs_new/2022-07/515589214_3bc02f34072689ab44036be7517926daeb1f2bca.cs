using DataBaseLayer.Employee;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace BussinessLayer.Interface
{
    public interface IEmployeeBL
    {
        Task AddEmployee(EmployeePostModel EmployeePostModel);
        Task UpdateEmployee( int EmployeeId, UpdateModel updateModel);

        Task DeleteEmployee(int EmployeeId);
    }
}