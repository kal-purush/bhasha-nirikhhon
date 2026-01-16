namespace TsundokuTraducoes.Helpers.DTOs.Public.Retorno
{
    public class RetornoCapituloNoVolume
    {
        public Guid VolumeId { get; set; }
        public Guid Id { get; set; }
        public string Numero { get; set; }
        public string Parte { get; set; }
        public int OrdemCapitulo { get; set; }
        public string Slug { get; set; }
        public DateTime DataInclusao { get; set; }
        public string Titulo { get; set; }
        public bool Publicado { get; set; }
    }
}