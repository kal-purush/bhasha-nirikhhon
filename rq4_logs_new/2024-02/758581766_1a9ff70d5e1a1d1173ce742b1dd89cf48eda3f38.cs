namespace user_monitoring.Models
{
    public class Setting
    {
        private string _statisticFilePath;

        public bool ReadKeyboardInput { get; set; }

        public bool ReadRunPrograms { get; set; }

        public bool AnalysisProhibitedWords { get; set; }

        public bool AnalysisProhibitedPrograms { get; set; }

        public string GetStatisticFilePath()
        {
            return _statisticFilePath;
        }

        public Setting(string statisticFilePath)
        {
            _statisticFilePath = statisticFilePath;
        }
    }
}