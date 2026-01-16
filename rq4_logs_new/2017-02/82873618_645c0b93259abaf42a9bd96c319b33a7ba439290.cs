using System;

namespace Aula4
{
    internal sealed class Produto
    {
        public Guid Id { get; private set; }
        public string Nome { get; private set; }
        public decimal Valor => (new Random().Next(1000, 3000));
        public string Url { get; private set; }

        public Produto(string nome, string url)
        {
            Id = Guid.NewGuid();
            Nome = nome;
            Url = url;
        }
    }
}