using System.Collections.Generic;

namespace Logatti.ByBus.Domain.Entities
{
    public class Empresa
    {
        public Empresa()
        {
        }

        public Empresa(string cnpj, string nome)
        {
            CNPJ = cnpj;
            Nome = nome;
        }

        public string CNPJ { get; set; }
        public string Nome { get; set; }
        public ICollection<Onibus> Onibus { get; set; }
    }
}