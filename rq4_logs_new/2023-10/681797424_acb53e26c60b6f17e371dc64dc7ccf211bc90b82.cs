using Microsoft.AspNetCore.Mvc;
using HOTELAPI1.Collections;
using HOTELAPI1.Services;
using System.Threading.Tasks;
using MongoDB.Bson;
using System.Security.Claims;

namespace HOTELAPI1.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ComentariosController : ControllerBase
    {
        private readonly ComentarioService _comentarioService;
        private readonly PropiedadService _propiedadService; // Asegúrate de tener un servicio similar

        public ComentariosController(ComentarioService comentarioService, PropiedadService propiedadService) // Inyecta aquí
        {
            _comentarioService = comentarioService;
            _propiedadService = propiedadService; // Asigna el servicio inyectado a la variable de la clase
        }


        [HttpPost]
        public async Task<IActionResult> CreateComentario([FromBody] Comentario comentario)
        {
            // Obtener el ClienteId del usuario logueado con Clerk
            var clienteId = User.FindFirstValue(ClaimTypes.NameIdentifier);
            if (string.IsNullOrEmpty(clienteId))
               // return Unauthorized();

            comentario.ClienteId = clienteId;

            // Validar que el PropiedadId existe
            var propiedad = await _propiedadService.GetPropiedadById(comentario.PropiedadId);
            if (propiedad == null)
                return BadRequest("PropiedadId no válido");

            await _comentarioService.CreateComentario(comentario);

            return CreatedAtRoute("GetComentario", new { id = comentario.Id.ToString() }, comentario);
        }


        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateComentario(string id, [FromBody] Comentario comentario)
        {
            if (comentario == null)
                return BadRequest("Comentario is required");

            var existingComentario = await _comentarioService.GetComentarioById(new ObjectId(id));
            if (existingComentario == null)
                return NotFound("Comentario not found");

            await _comentarioService.UpdateComentario(new ObjectId(id), comentario);

            return NoContent(); // Retorna un 204 No Content, que es una práctica común después de un PUT exitoso.
        }

        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteComentario(string id)
        {
            var comentario = await _comentarioService.GetComentarioById(new ObjectId(id));
            if (comentario == null)
                return NotFound("Comentario not found");

            await _comentarioService.DeleteComentario(new ObjectId(id));

            return NoContent(); // Retorna un 204 No Content, que es una práctica común después de un DELETE exitoso.
        }
        [HttpGet("{id}", Name = "GetComentario")]
        public async Task<ActionResult<Comentario>> GetComentario(ObjectId id)
        {
            if (id == ObjectId.Empty)
                return BadRequest("Id is required");

            var comentario = await _comentarioService.GetComentarioById(id);
            if (comentario == null)
            {
                return NotFound();
            }
            return Ok(comentario);
        }
        [HttpGet]
        public async Task<ActionResult<IEnumerable<Comentario>>> GetComentarios()
        {
            var comentarios = await _comentarioService.GetComentarios();
            return Ok(comentarios);
        }



    }
}