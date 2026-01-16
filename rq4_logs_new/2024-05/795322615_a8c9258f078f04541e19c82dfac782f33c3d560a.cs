using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using api.Interfaces;
using api.Mapper;
using Microsoft.AspNetCore.Mvc;

namespace api.Controllers
{
    [Route("api/deposito")]
    [ApiController]
    public class DepositoController : ControllerBase
    {
        private readonly IDepositoRepository _depositoRepo;
        public DepositoController(IDepositoRepository depositoRepo)
        {
            _depositoRepo = depositoRepo;
        }

        [HttpGet]
        public async Task<IActionResult> GetAll()
        {
            var depositos = await _depositoRepo.GetAllAsync();
            var depositoDto = depositos.Select(d => d.ToDepositoDto());
            return Ok(depositoDto);
        }
    }
}