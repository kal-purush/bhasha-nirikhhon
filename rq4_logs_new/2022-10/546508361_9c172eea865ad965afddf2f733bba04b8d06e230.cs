using MediatR;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.FridgeProduct.Commands.CreateFridgeProduct
{
    public class CreateFridgeProductCommand : IRequest<Guid>
    {
        public Guid FridgeId { get; set; }
        public Guid ProductId { get; set; }
        [Range(0, int.MaxValue, ErrorMessage = "Quantity can't be lower than 0")]
        public int? Quantity { get; set; }
    }
}