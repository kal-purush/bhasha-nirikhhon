using Levva.Newbies.Coins.Logic.Dtos;
using Levva.Newbies.Coins.Logic.Interfaces;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Levva.Newbies.Coins.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CategoriaController : ControllerBase
    {
        private readonly ICategoriaService _service;
        public CategoriaController(ICategoriaService service)
        {
            _service = service;
        }

        [HttpPost]
        public IActionResult Create(CategoriaDto categoria)
        {
            _service.Create(categoria);
            return Created("", categoria);
        }

        [HttpGet]
        public ActionResult<CategoriaDto> Get(int Id)
        {
            return _service.Get(Id);
        }

        [HttpGet("list")]
        public ActionResult<List<CategoriaDto>> Get()
        {
            return _service.GetAll();
        }

        [HttpPut]
        public IActionResult Update(CategoriaDto categoria)
        {
            _service.Update(categoria);
            return Ok();
        }

        [HttpDelete]
        public IActionResult Delete(int Id)
        {
            _service.Delete(Id);
            return Ok();
        }
    }

}