using API.Database.Models;
using API.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Net.Mime;

namespace API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [Produces(
        MediaTypeNames.Application.Json,
        MediaTypeNames.Application.Xml
        )]
    [Consumes( MediaTypeNames.Application.Json )]   
    public class ProdutosController : ControllerBase
    {
        private readonly ProdutoService _produtoService;
        public ProdutosController(ProdutoService produtoService) {

            _produtoService = produtoService;
        }

        [HttpGet()]
        [Produces(MediaTypeNames.Application.Json)]
        [Consumes(MediaTypeNames.Application.Json)]

        public ActionResult<List<Produto>> GetAll() { 
        
            return Ok(_produtoService.GetAll());
        }

        [HttpGet(":codigo")]
        [Produces(MediaTypeNames.Application.Json)]
        [Consumes(MediaTypeNames.Application.Json)]
        public ActionResult<Produto> GetByCodigo(int codigo)
        {
            try
            {
                var produto = _produtoService.GetById(codigo);
                return Ok(produto);
            }
            catch(NotFoundException)
            {
                return NotFound("Produto não encontrado");
            }
            catch(Exception ex)
            {
                return BadRequest("Ocorreu um problema ao acessar o produto." + ex.Message);
            }
        }
    }
}
