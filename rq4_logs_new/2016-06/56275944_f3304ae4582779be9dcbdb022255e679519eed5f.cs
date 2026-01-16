using System;

namespace SuperERP.DAL.Models
{
    public class OrdemServico
    {
        public int Id { get; set; }
        public string Nome { get; set; }
        public int IdServico { get; set; }
        public int IdStatus { get; set; }
        public string NumeroOs { get; set; }
        public DateTime DataI_Inicio { get; set; }
        public DateTime DataI_Entrega { get; set; }
        public string Equipamento_Recebido { get; set; }
        public string NumeroSerie { get; set; }
        public string Marca { get; set; }
        public string Modelo { get; set; }
        public string ObsRecebimento { get; set; }
        public string ObsProblema { get; set; }
        public string DescrServico { get; set; }
        public string ObsInterno { get; set; }
        public virtual Servico Servico { get; set; }
        public virtual StatusServico Status_Servico { get; set; }
    }
}