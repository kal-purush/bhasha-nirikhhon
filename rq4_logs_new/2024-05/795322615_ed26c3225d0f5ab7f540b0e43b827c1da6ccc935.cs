using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using api.Data;
using api.Interfaces;
using api.Mapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace api.Controllers
{
    [Route("api/ferreteria")]
    [ApiController]
    public class FerreteriaController : ControllerBase
    {
        private readonly IFerreteriaRepository _ferreteriaRepo;
        private readonly ApplicationDbContext _context;
        public FerreteriaController(IFerreteriaRepository ferreteriaRepo, ApplicationDbContext context)
        {
            _ferreteriaRepo = ferreteriaRepo;
            _context = context;
        }

        [HttpGet]
        public async Task<IActionResult> GetAll()
        {
            var ferreterias = await _context.Ferreterias.ToListAsync();
            var ferreteriaDto = ferreterias.Select(f => f.ToFerreteriaDto());
            return Ok(ferreteriaDto);
        } 
    }
}