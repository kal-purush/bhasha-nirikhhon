using VanKassa.Domain.Dtos;
using VanKassa.Domain.Enums;

namespace VanKassa.Backend.Core.Data.EmployeesSort.ConcreteStrategies;

public class AddressesSortConcreteStrategy : IEmployeesSortStrategy
{
    public IEnumerable<EmployeesDbDto> SortEmployees(IEnumerable<EmployeesDbDto> employees, SortDirection sortDirection)
        => sortDirection switch
        {
            SortDirection.None => employees.OrderBy(emp => emp.UserId),
            SortDirection.Ascending => employees.OrderBy(emp => emp.Addresses),
            SortDirection.Descending => employees.OrderByDescending(emp => emp.Addresses),
            _ => throw new ArgumentOutOfRangeException(nameof(sortDirection), sortDirection, null)
        };
}