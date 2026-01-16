namespace TaskManagementSystem.Models
{
    public class DashboardModel
    {
        public int Id { get; set; }
        public string Title { get; set; }
        public DateTime Deadline { get; set; }
        public string Status { get; set; }
        public string AssignedUsers { get; set; }
        public string Comment { get; set; }
    }
}