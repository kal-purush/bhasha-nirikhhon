namespace HelpMyStreet.Contracts
{
    public class NewsTickerMessage
    {
        public string Message { get; set; }
        public Utils.Enums.SupportActivities? SupportActivity { get; set; }
        public double? Number { get; set; }
    }
}