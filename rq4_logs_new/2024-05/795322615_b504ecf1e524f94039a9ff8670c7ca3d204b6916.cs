using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using api.Dtos.Marca;
using api.Interfaces;
using api.Mapper;
using Microsoft.AspNetCore.Mvc;

namespace api.Controllers
{   
    [Route("api/marca")]
    [ApiController]
    public class MarcaController : ControllerBase
    {
        private readonly IMarcaRepository _marcaRepo;
        private readonly IProveedorRepository _proveedorRepo;
        public MarcaController(IMarcaRepository marcaRepo, IProveedorRepository proveedorRepo)
        {
            _marcaRepo = marcaRepo;
            _proveedorRepo = proveedorRepo;
        }

        [HttpGet]
        public async Task<IActionResult> GetAll()
        {
            var marcas = await _marcaRepo.GetAllAsync();
            var marcasDto = marcas.Select(m => m.ToMarcaDto());
            return Ok(marcasDto);
        }

        [HttpGet]
        [Route("{id:int}")]
        public async Task<IActionResult> GetById([FromRoute] int id)
        {
            if(!ModelState.IsValid) 
                return BadRequest(ModelState);

            var marca = await _marcaRepo.GetByIdAsync(id);
            if(marca == null) return NotFound();

            return Ok(marca.ToMarcaDto());
        }

        [HttpPost("{proveedorId:int}")]
        public async Task<IActionResult> Post([FromRoute] int proveedorId, CreateMarcaRequestDto marcaDto)
        {
            if(!ModelState.IsValid) 
                return BadRequest(ModelState);
            
            if(!await _proveedorRepo.ProveedorExists(proveedorId))
            {
                return BadRequest("El proveedor ingresado no existe!!");
            }

            var marcaModel = marcaDto.ToMarcaFromCreate(proveedorId);
            await _marcaRepo.CreateAsync(marcaModel);
            return CreatedAtAction(nameof(GetById), new{id = marcaModel.Id}, marcaModel.ToMarcaDto());
        }

        [HttpPut]
        [Route("{id:int}")]
        public async Task<IActionResult> Update([FromRoute] int id, UpdateMarcaRequestDto updateDto)
        {
            if(!ModelState.IsValid) 
                return BadRequest(ModelState);
            
            var marca = await _marcaRepo.UpdateAsync(id, updateDto.ToMarcaFromUpdate());
            if(marca == null) return NotFound("La marca que desea actualizar no existe!!");

            return Ok(marca.ToMarcaDto());
        }

        [HttpDelete]
        [Route("{id:int}")]
        public async Task<IActionResult> Delete([FromRoute] int id)
        {
            if(!ModelState.IsValid) 
                return BadRequest(ModelState);

            var marcaModel = await _marcaRepo.DeleteAsync(id);
            if(marcaModel == null) return NotFound("La marca que desea eliminar no existe!!");

            return Ok(marcaModel);
        }
    }
}