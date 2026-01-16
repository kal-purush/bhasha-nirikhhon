using CrudProduto.Data;
using CrudProduto.Models;
using CrudProduto.ViewModels;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace CrudProduto.Controllers
{

    [ApiController]
    public class PedidoController : ControllerBase
    {

        [HttpGet("pedidos")]
        public IActionResult Get(
            [FromServices] AppDbContext context)
        {
            var pedidos = context.Pedidos.ToList();
            return Ok(pedidos);
        }

        [HttpGet("pedidos/{id:int}")]
        public IActionResult GetById(
           [FromRoute] int id,
           [FromServices] AppDbContext context)
        {
            var pedido = context.Pedidos
                .Include(x => x.ItensPedido)
                .ThenInclude(y => y.Produto)
                .Where(x => x.Id == id).FirstOrDefault();


            if (pedido == null)
                return NotFound();

            var valorTotal = 0.0;
            foreach (var item in pedido.ItensPedido)
            {
               var valorTotalItem = item.Quantidade * item.Produto.Valor;
               valorTotal += valorTotalItem;
            }

            var response = new GetPedidoViewModel
            {
                Id = pedido.Id,
                NomeCliente = pedido.NomeCliente,
                EmailCliente = pedido.EmailCliente,
                Pago = pedido.Pago,
                ValorTotal = valorTotal,
                ItensPedido = pedido.ItensPedido.Select(x => new GetItensPedidoViewModel
                {
                    Id = x.Id,
                    IdProduto = x.IdProduto,
                    NomeProduto = x.Produto.NomeProduto,
                    ValorUnitario = x.Produto.Valor,
                    Quantidade = x.Quantidade

                }).ToList()
            };

            return Ok(response);
        }

        [HttpPost("pedidos")]
        public IActionResult Post(
           [FromBody] CreatePedidoViewModel model,
           [FromServices] AppDbContext context)
        {
                var pedidos = new Pedido
                {
                    NomeCliente = model.NomeCliente,
                    EmailCliente = model.EmailCliente,
                    Pago = model.Pago,
                    DataCriacao = DateTime.Now,
                    ItensPedido = model.ItensPedido.Select(x => new ItensPedido()
                    {
                        IdProduto = x.IdProduto,
                        Quantidade = x.Quantidade
                    }).ToList()
                };

                context.Pedidos.Add(pedidos);
                context.SaveChanges();

                model.Id = pedidos.Id;

                return Created($"pedidos/{pedidos.Id}", model);
            
        }

        [HttpPut("pedidos/{id:int}")]
        public IActionResult Put(
           [FromRoute] int id,
           [FromBody] PutPedidoViewModel model,
           [FromServices] AppDbContext context)
        {

            var pedido = context.Pedidos
               .Include(x => x.ItensPedido)
               .ThenInclude(y => y.Produto)
               .Where(x => x.Id == id).FirstOrDefault();

            if(pedido == null)
                return NotFound();

            var pedidos = new Pedido
            {
                NomeCliente = model.NomeCliente,
                EmailCliente = model.EmailCliente,
                Pago = model.Pago,
                DataCriacao = DateTime.Now,
                ItensPedido = model.ItensPedido.Select(x => new ItensPedido()
                {
                    IdProduto = x.IdProduto,
                    Quantidade = x.Quantidade
                }).ToList()
            };

            context.Pedidos.Update(pedidos);
            context.SaveChanges();

            return Created($"pedidos/id/{pedidos.Id}", model);
        }

        [HttpDelete("pedidos/{id:int}")]
        public IActionResult Delete(
          [FromRoute] int id,
          [FromServices] AppDbContext context)
        {
            var pedidos = context.Pedidos.FirstOrDefault(x => x.Id == id);

            if (pedidos == null)
                return NotFound();

            context.Pedidos.Remove(pedidos);
            context.SaveChanges();

            return Ok("Deletado com sucesso!");
        }

    }    
}