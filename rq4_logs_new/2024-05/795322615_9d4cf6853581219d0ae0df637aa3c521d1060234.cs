using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using api.Dtos.DetalleDeMovimiento;
using api.Interfaces;
using api.Mapper;
using Microsoft.AspNetCore.Mvc;

namespace api.Controllers
{
    [Route("api/detalle")]
    [ApiController]
    public class DetalleDeMovimientosController : ControllerBase
    {
        private readonly IDetalleDeMovimientosRepository _detalleRepo;
        private readonly IProductoRepository _productoRepo;
        private readonly IMovimientoRepository _movimientoRepo;
        public DetalleDeMovimientosController(IDetalleDeMovimientosRepository detalleRepo, IProductoRepository productoRepo, IMovimientoRepository movimientoRepo)
        {
            _detalleRepo = detalleRepo;
            _productoRepo = productoRepo;
            _movimientoRepo = movimientoRepo;
        }

        [HttpGet]
        public async Task<IActionResult> GetAll()
        {
            var detalles = await _detalleRepo.GetAllAsync();
            var detalleDto = detalles.Select(f => f.ToDetalleDeMovimientoDto());
            return Ok(detalleDto);
        } 

        [HttpGet]
        [Route("{id:int}")]
        public async Task<IActionResult> GetById([FromRoute] int id)
        {
            if(!ModelState.IsValid) 
                return BadRequest(ModelState);

            var detalle = await _detalleRepo.GetByIdAsync(id);
            if(detalle == null) return NotFound();

            return Ok(detalle);
        }

        [HttpPost]
        [Route("{movimientoId:int}/{productoId}")]
        public async Task<IActionResult> Post([FromBody] CreateDetalleRequestDto detalleDto, [FromRoute] int movimientoId, [FromRoute] int productoId)
        {
            if(!ModelState.IsValid) 
                return BadRequest(ModelState);

            if(!await _movimientoRepo.MovimientoExists(movimientoId))
            {
                return BadRequest("El movimiento ingresado no existe!!");
            }
            var movimiento = await _movimientoRepo.GetByIdAsync(movimientoId);

            if(!await _productoRepo.ProductoExists(productoId) )
            {
                return BadRequest("El producto ingresado no existe!!");
            }

            var detalleModel = detalleDto.ToDetalleFromCreate(movimientoId, productoId);

            await _detalleRepo.CreateAsync(detalleModel);
            return CreatedAtAction(nameof(GetById), new{id = detalleModel.Id}, detalleModel.ToDetalleDeMovimientoDto());
        }

        [HttpPut]
        [Route("{id:int}")]
        public async Task<IActionResult> Update([FromRoute] int id, [FromBody] UpdateDetalleRequestDto updateDto)
        {
            if(!ModelState.IsValid) 
                return BadRequest(ModelState);

            var detalleModel = await _detalleRepo.UpdateAsync(id, updateDto.ToDetalleFromUpdate());
            if(detalleModel == null) return NotFound();

            return Ok(detalleModel.ToDetalleDeMovimientoDto());
        }
    }
}