namespace MyBoards.DTO
{
    public class EpicDto
    {
        public int Id { get; set; }
        public int Priority { get; set; }
        public string Area { get; set; }
        public DateTime? StartDate { get; set; }
        public string AuthorFullName { get; set; }
    }
}