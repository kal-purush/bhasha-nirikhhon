using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TiendaAPI.Common.DTO.Enums;

namespace TiendaAPI.Common.DTO.Request
{
    public class PedidoRequest
    {
        public int ClienteId { get; set; }
        public List<ProductoPedidoRequest> ProductosPedido { get; set; }
        public EstadoPedidoEnum Estado { get; set; }
    }
}