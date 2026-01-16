namespace WolfDen.Application.DTOs.Attendence
{
    public class SubOrdinateDTO
    {
        public int Id { get; set; }
        public int EmployeeCode { get; set; }
        public string Name { get; set; }
        public string? Email { get; set; }
        public string? Photo { get; set; }
        public string? Department { get; set; }
        public string? Manager {  get; set; }
        public string? Designation { get; set; }
        public List<SubOrdinateDTO>? SubOrdinates { get; set; }
    }
}