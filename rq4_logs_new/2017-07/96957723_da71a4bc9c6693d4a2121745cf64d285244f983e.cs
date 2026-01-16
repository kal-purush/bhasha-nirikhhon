namespace PontoEmail.Lib.Domain
{
    internal class ItemDeEnvio
    {
        public string Entrada { get; }
        public string Saida { get; }

        public ItemDeEnvio(string entrada, string saida)
        {
            Entrada = entrada;
            Saida = saida;
        }
    }
}