using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using api.Interfaces;
using api.Mapper;
using Microsoft.AspNetCore.Mvc;

namespace api.Controllers
{
    [Route("api/proveedor")]
    [ApiController]
    public class ProveedorController : ControllerBase
    {
        private readonly IProveedorRepository _proveedorRepo;
        public ProveedorController(IProveedorRepository proveedorRepo)
        {
            _proveedorRepo = proveedorRepo;
        }

        [HttpGet]
        public async Task<IActionResult> GetAll()
        {
            var proveedores = await _proveedorRepo.GetAllAsync();
            var proveedoresDto = proveedores.Select(p => p.ToProveedorDto());
            return Ok(proveedoresDto);
        }
    }
}