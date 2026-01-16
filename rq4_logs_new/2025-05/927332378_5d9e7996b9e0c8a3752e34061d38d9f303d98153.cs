using System.Text.Json.Serialization;

namespace mangadex.Models;

    public class MangaResponse
    {
        [JsonPropertyName("result")]
        public string Result { get; set; }
        
        [JsonPropertyName("data")]
        public List<MangaData> Data { get; set; }
    
    }

    public class MangaData
    {
        [JsonPropertyName("id")]
        public string Id { get; set; }
        
        [JsonPropertyName("type")]
        public string Type { get; set; }
        
        [JsonPropertyName("attributes")]
        public MangaAttributes Attributes { get; set; }
    }

    public class MangaAttributes
    {
        public Dictionary<string, string> Title { get; set; }
        public string Status { get; set; }

        public string ObterTitulo()
        {
            if (Title == null)
                return "Título não disponível";

            if (Title.TryGetValue("pt-br", out var tituloPt))
                return tituloPt;

            if (Title.TryGetValue("en", out var tituloEn))
                return tituloEn;

            return Title.Values.FirstOrDefault() ?? "Título não disponível";
        }
    } 


