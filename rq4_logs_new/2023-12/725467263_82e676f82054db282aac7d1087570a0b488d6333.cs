namespace toiec_web.ViewModels.TestQuestionUnit
{
    public class TestQuestionUnitUpdateMapModel
    {
        public Guid idTest { get; set; }
        public Guid idTestPart { get; set; }
        public string? paragraph { get; set; }
        public IFormFile? newAudio { get; set; }
        public string? oldAudio { get; set; }
        public IFormFile? newImage { get; set; }
        public string? oldImage { get; set; }
        public string? script { get; set; }
        public string? translation { get; set; }
    }
}