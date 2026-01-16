using System;

namespace DailyNews
{
    public class Headline
    {
        public string HeadlineText { get; private set; }
        public string Texture { get; private set; }
        public string Source { get; private set; }

        public Headline(string headlineText, string texture, string source)
        {
            this.HeadlineText = headlineText;
            this.Texture = texture;
            this.Source = source;
        }
    }
}