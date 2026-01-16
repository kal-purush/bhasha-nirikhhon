namespace toiec_web.Models
{
    public class RecordModel
    {
        public Guid idRecord { get; set; }
        public Guid idTest { get; set; }
        public Guid idStudent { get; set; }
        public DateTime createDate { get; set; }
        public int listenCorrect { get; set; }
        public int listenScore { get; set; }
        public int readingCorrect { get; set; }
        public int readScore { get; set; }
        public int correctAns { get; set; }
        public int wrongAns { get; set; }
        public int totalScore { get; set; }
    }
}