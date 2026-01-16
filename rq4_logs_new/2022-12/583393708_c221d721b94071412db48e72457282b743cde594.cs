namespace LabWork8
{
    public class Subtitle
    {
        public int StartTime { get; }
        public int EndTime { get; }
        public string Position { get; }
        public ConsoleColor Color { get; }
        public string Text { get; }

        public Subtitle(int startTime, int endTime, string position, ConsoleColor color, string text)
        {
            StartTime = startTime;
            EndTime = endTime;
            Position = position;
            Color = color;
            Text = text;
        }

    }

}