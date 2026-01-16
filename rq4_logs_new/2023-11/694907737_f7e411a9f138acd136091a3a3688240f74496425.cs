namespace DnD_NPC_Generator.Models
{
    public class APISpellModel
    {
        public string index { get; set; }
        public string name { get; set; }
        public List<string> description { get; set; }
        public List<string> higherLevel { get; set; }
        public string range { get; set; }
        public List<string> components { get; set; }
        public string material { get; set; }
        public bool ritual { get; set; }
        public string duration { get; set; }
        public bool concentration { get; set; }
        public string castingTime { get; set; }
        public int level { get; set; }
        public string attackType { get; set; }
        public Dictionary<string, List<string>> damage { get; set;}
        public Dictionary<string, string> school { get; set; }
        public List<Dictionary<string, string>> classes { get; set; }
        public List<Dictionary<string, string>> subclasses { get; set; }
        public string url { get; set; }
    }
}