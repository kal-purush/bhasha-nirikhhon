namespace hotelier_core_app.Model.DTOs.Request
{
    public class AddUserToPolicyGroupDTO
    {
        public long UserId { get; set; }
        public long PolicyGroupId { get; set; }
    }
}