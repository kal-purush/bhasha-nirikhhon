using TsundokuTraducoes.Helpers.DTOs.Admin;

namespace TsundokuTraducoes.Helpers.DTOs.Public.Retorno
{
    public class RetornoCapituloComic
    {
        public Guid Id { get; set; }
        public string Numero { get; set; }
        public string Parte { get; set; }
        public string Slug { get; set; }
        public string UsuarioInclusao { get; set; }
        public string UsuarioAlteracao { get; set; }
        public string DataInclusao { get; set; }
        public string DataAlteracao { get; set; }
        public int OrdemCapitulo { get; set; }
        public string DescritivoCapitulo => TratamentoDeStrings.RetornaDescritivoCapitulo(Numero, Parte);
        public List<EnderecoImagemDTO> ListaImagens { get; set; }
    }
}