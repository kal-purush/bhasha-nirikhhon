using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static WolfDen.Domain.Enums.EmployeeEnum;
using WolfDen.Domain.Entity;

namespace WolfDen.Application.DTOs
{
    public class EmployeeDTO
    {
        public int Id { get;  set; }
        public int EmployeeCode { get;  set; }

        public string RFId { get;  set; }
        public string? FirstName { get;  set; }
        public string? LastName { get;  set; }
        public string? Email { get;  set; }
        public string? PhoneNumber { get;  set; }
        public DateOnly? DateofBirth { get;  set; }

        public DateOnly? JoiningDate { get;  set; }
        public Gender? Gender { get;  set; }
        public int? DesignationId { get;  set; }
        public Designation? Designation { get;  set; }
        public int? DepartmentId { get;  set; }
        public string? DepartmentName { get;  set; }
        public int? ManagerId { get;  set; }
        public string  ManagerName { get;  set; }
        public bool? IsActive { get;  set; }
    }
}