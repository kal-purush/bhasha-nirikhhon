namespace DnD_NPC_Generator.Models
{
    public class Spell
    {
        public string Name { get; set; }
        public ICollection<string> Desc { get; set; }
        public ICollection<string> Higher_level { get; set; }
        public string Range { get; set; }
        public ICollection<string> Components { get; set; }
        public string Material { get; set; }
        public bool Ritual { get; set; }
        public string Duration { get; set; }
        public bool Concentration { get; set; }
        public string Casting_time { get; set; }
        public int Level { get; set; }
        public string Attack_type { get; set; }
        public Dictionary<string, string> School { get; set; }
        public ICollection<Dictionary<string, string>> Classes { get; set; }
    }
}