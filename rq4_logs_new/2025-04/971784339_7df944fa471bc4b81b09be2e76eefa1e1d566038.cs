using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.IO;


namespace modul7_kelompok_3
{
    public class GlossaryItem_2311104060
    {
        public Glossary glossary { get; set; }

        public static void ReadJSON()
        {
            string json = File.ReadAllText("jurnal7_3_2311104060.json");
            var data = JsonConvert.DeserializeObject<GlossaryItem_2311104060>(json);

            var glossEntry = data?.glossary?.GlossDiv?.GlossList?.GlossEntry;

            if (glossEntry != null)
            {
                Console.WriteLine("GlossEntry:");
                Console.WriteLine($"ID: {glossEntry.ID}");
                Console.WriteLine($"GlossTerm: {glossEntry.GlossTerm}");
                Console.WriteLine($"Acronym: {glossEntry.Acronym}");
                Console.WriteLine($"Abbrev: {glossEntry.Abbrev}");
                Console.WriteLine($"GlossDef.Para: {glossEntry.GlossDef.para}");
            }
        }
    }

    public class Glossary
    {
        public string title { get; set; }
        public GlossDiv GlossDiv { get; set; }
    }

    public class GlossDiv
    {
        public string title { get; set; }
        public GlossList GlossList { get; set; }
    }

    public class GlossList
    {
        public GlossEntry GlossEntry { get; set; }
    }

    public class GlossEntry
    {
        public string ID { get; set; }
        public string SortAs { get; set; }
        public string GlossTerm { get; set; }
        public string Acronym { get; set; }
        public string Abbrev { get; set; }
        public GlossDef GlossDef { get; set; }
        public string GlossSee { get; set; }
    }

    public class GlossDef
    {
        public string para { get; set; }
        public List<string> GlossSeeAlso { get; set; }
    }

}