using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SuperERP.Financeiro.DTO
{
    class ViewCompraDTO
    {
        public int ID_PARC { get; set; }
        public int ID_Compra { get; set; }
        public int Numero_Parcela { get; set; }
        public decimal Valor { get; set; }
        public int Pago { get; set; }
        public DateTime Data_Pagamento { get; set; }
        public DateTime Data_Pago { get; set; }
        public int ID_Empresa { get; set; }
        public int ID_Fornecedor { get; set; }
        public int ID_Usuario { get; set; }
        public int ID_Status { get; set; }
        public int Compra_Num { get; set; }
        public DateTime Data_Compra { get; set; }
        public decimal Desconto { get; set; }
        public int ID_FormaPgto { get; set; }
        public int ID_Conta { get; set; }
        public string Observacoes { get; set; }
    }
}