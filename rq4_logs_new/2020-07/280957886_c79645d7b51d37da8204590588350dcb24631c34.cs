using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using WebApplication.Data;
using WebApplication.Models;

// Gustavo Anderson

namespace WebApplication.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class EnderecoController : ControllerBase
    {
        private readonly EnderecoContext _context;

        public EnderecoController(EnderecoContext context)
        {
            _context = context;

            if (_context.Enderecos.Count() == 0) 
            {
                _context.Enderecos.Add(new Endereco { Bairro = "Cidade Industrial de Curitiba", CEP = "80740-050", Complemento = "", Localidade = "Curitiba", Logradouro = "Rua Professor Pedro Viriato Parigot de Souza", UF = "PR" });
                _context.SaveChanges();
            }
        }

        // GET: api/Endereco/ListarEnderecos
        [HttpGet("ListarEnderecos")]
        public async Task<ActionResult<IEnumerable<Endereco>>> GetEnderecoItems()
        {
            return await _context.Enderecos.ToListAsync();
        }

        // GET: api/Endereco/ListarEndereco/5
        [HttpGet("ListarEndereco/{id}")]
        public async Task<ActionResult<Endereco>> GetEnderecoItem(long id)
        {
            var endereco = await _context.Enderecos.FindAsync(id);

            if (endereco == null)
            {
                return NotFound();
            }

            return endereco;
        }

        // POST: api/Endereco/CadastrarEndereco
        [HttpPost("CadastrarEndereco")]
        public async Task<ActionResult<Endereco>> PostEndereco(Endereco item)
        {
            _context.Enderecos.Add(item);
            await _context.SaveChangesAsync();

            return CreatedAtAction(nameof(GetEnderecoItem), new { id = item.EnderecoId }, item);
        }

        // PUT: api/AlterarEndereco/5
        [HttpPut("AlterarEndereco")]
        public async Task<IActionResult> PutEnderecoItem(Endereco item)
        {
            _context.Entry(item).State = EntityState.Modified;
            await _context.SaveChangesAsync();

            return NoContent();
        }

        // DELETE: api/DeletarEndereco/5
        [HttpDelete("DeletarEndereco/{id}")]
        public async Task<IActionResult> DeleteEnderecoItem(long id)
        {
            var item = await _context.Enderecos.FindAsync(id);

            if (item == null)
            {
                return NotFound();
            }

            _context.Enderecos.Remove(item);
            await _context.SaveChangesAsync();

            return NoContent();
        }
    }
}