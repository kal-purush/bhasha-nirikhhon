namespace WolfDen.Application.DTOs
{
    public class EmployeeHierarchyDto
    {
        public int Id { get; set; }
        public int EmployeeCode { get; set; }
        public string? FirstName { get; set; }
        public string? LastName { get; set; }
        public string? Email { get; set; }
        public string? PhoneNumber { get; set; }
        public DateOnly? DateofBirth { get; set; }
        public int? DesignationId { get; set; }
        public int? DepartmentId { get; set; }
        public int? ManagerId { get; set; }

        public List<EmployeeHierarchyDto> Subordinates { get; set; }
    }
}