using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Api.Backend.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Api.Backend.Domain.Models;
using Api.Backend.Data.Dtos.Produto;
using Api.Backend.Data.Dtos.Produto_Campanha;

namespace Api.Backend.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class Produto_CampanhaController : ControllerBase
    {
        private AppDbContext _context;
        private IMapper _mapper;

        public Produto_CampanhaController(AppDbContext context, IMapper mapper)
        {
            _context = context;
            _mapper = mapper;
        }


        [HttpPost]
        public IActionResult AdicionaProduto([FromBody] CreateProduto_CampanhaDto produto_campanhaDto)
        {
            Produto_Campanha produto_Campanha = _mapper.Map<Produto_Campanha>(produto_campanhaDto);
            _context.Produto_Campanhas.Add(produto_Campanha);
            _context.SaveChanges();
            return CreatedAtAction(nameof(RecuperaProdutosPorId), new { Id = produto_Campanha.Id }, produto_Campanha);
        }

        [HttpGet]
        public IEnumerable<Produto_Campanha> RecuperaProduto_Campanhas([FromQuery] string nomeDoProduto)
        {
            return _context.Produto_Campanhas;
        }

        [HttpGet("{id}")]
        public IActionResult RecuperaProdutosPorId(int id)
        {
            Produto_Campanha produto_campanha = _context.Produto_Campanhas.FirstOrDefault(produto_campanha => produto_campanha.Id == id);
            if (produto_campanha != null)
            {
                ReadProduto_CampanhaDto produto_campanhaDto = _mapper.Map<ReadProduto_CampanhaDto>(produto_campanha);
                return Ok(produto_campanhaDto);
            }
            return NotFound();
        }

        [HttpPut("{id}")]
        public IActionResult AtualizaProduto_Campanha(int id, [FromBody] UpdateProduto_CampanhaDto produto_campanhaDto)
        {
            Produto_Campanha produto_campanha = _context.Produto_Campanhas.FirstOrDefault(produto_campanha => produto_campanha.Id == id);
            if (produto_campanha == null)
            {
                return NotFound();
            }
            _mapper.Map(produto_campanhaDto, produto_campanha);
            _context.SaveChanges();
            return NoContent();
        }


        [HttpDelete("{id}")]
        public IActionResult DeletaProduto_Campanha(int id)
        {
            Produto_Campanha produto_campanha = _context.Produto_Campanhas.FirstOrDefault(produto_campanha => produto_campanha.Id == id);
            if (produto_campanha == null)
            {
                return NotFound();
            }
            _context.Remove(produto_campanha);
            _context.SaveChanges();
            return NoContent();
        }

    }
}