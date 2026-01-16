namespace ms_spa.Api.Contract.Cliente
{
    public class ClienteResponseContract
    {
        public int Id { get; set; }
        public string Nome { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        public string Cpf { get; set; } = string.Empty;
        public string Telefone { get; set; } = string.Empty;
        public string Endereco { get; set; } = string.Empty;
        public string Observacao { get; set; } = string.Empty;
        public int? UsuarioId { get; set; }
    }
}