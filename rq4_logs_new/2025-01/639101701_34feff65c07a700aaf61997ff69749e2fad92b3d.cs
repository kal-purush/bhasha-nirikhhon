using Newtonsoft.Json;

namespace TsundokuTraducoes.Helpers.DTOs.Public.Retorno
{
    public class RetornoComic
    {
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string UrlCapaPrincipal { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string UrlCapaVolume { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string UrlBanner { get; set; }
        
        public Guid Id { get; set; }
        public string Titulo { get; set; }
        public string TituloAlternativo { get; set; }
        public string Alias { get; set; } 
        public string Autor { get; set; }
        public string Artista { get; set; }
        public string Ano { get; set; }
        public string Visualizacoes { get; set; }
        public string Sinopse { get; set; }
        public Boolean EhRecomdacao { get; set; }
        public Boolean EhObraMaiorIdade { get; set; }
        public string DescritivoVolume { get; set; }
        public string Slug { get; set; }
        public string TipoObraSlug { get; set; }
        public string TipoObra { get; set; }
        public string StatusObraSlug { get; set; }
        public string StatusObra { get; set; }
        public string NacionalidadeSlug { get; set; }
        public string Nacionalidade { get; set; }
        public string Observacao { get; set; }
        public string UrlCapa { get; set; }
    }
}