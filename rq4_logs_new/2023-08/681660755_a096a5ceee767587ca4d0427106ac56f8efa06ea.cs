using Core.Utilities.Results;
using Entities.Concrete;
using System;
using System.Collections.Generic;
using System.Text;

namespace Business.Abstract
{
    public interface IEmployeeService
    {
        IResult Add(Employee employee0);
        IResult Delete(Employee employee0);
        IResult Update(Employee employee0);
        IDataResult<List<Employee>> GetAll();
        Employee GetById(int Id);
        Employee GetByName(string name);
        Employee GetByDepartment(string department);

    }
}