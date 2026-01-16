using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WpfApp3.MVVM.Model
{
    class Pedido
    {
        Pedido() { }

        [Key]
        private long Id { get; }

        [Required]
        private Pessoa Pessoa { get; set; }

        [Required]
        private Produto Produto { get; set; }


        private double ValorTotal { get; set; }

        private DateTime DataVenda { get; set; }

        [Required]
        private FormaPagamentoEnum FormaPagamento { get; set; }

        private StatusEnum Status { get; set; }

        private enum FormaPagamentoEnum
        {
            Dinheiro,
            Cartao,
            Boleto
        }

        private enum StatusEnum
        {
            Pendente,
            Pago,
            Enviado,
            Recebido
        }
    }
}