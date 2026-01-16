using System.Collections.Generic;

namespace BlightnautsDialogue
{
    class Area
    {
        public string Name;
        public List<Dialogue> Dialogues;
        public decimal TotalDuration
        {
            get
            {
                decimal result = 0;
                foreach (Dialogue dialogue in Dialogues)
                {
                    if (dialogue.End > result)
                    {
                        result = dialogue.End;
                    }
                }
                return result;
            }
        }

        public class Dialogue
        {
            public string Portrait, Content;
            public decimal Start, End;
            public decimal Duration { get => End - Start; }

            public Dialogue()
            {
                Portrait = string.Empty;
                Content = string.Empty;
                Start = 0;
                End = 0;
            }
        }

        public Area(string name)
        {
            Name = name;
            Dialogues = new List<Dialogue>();
            Dialogues.Add(new Dialogue());
        }
    }
}