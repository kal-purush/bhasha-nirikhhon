using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using WebApiKalum.Entities;
using WebApiKalum.Dtos;
using AutoMapper;
using WebApiKalum.Utilities;

namespace WebApiKalum.Controllers
{
    [ApiController]
    [Route("v1/KalumManagement/[controller]")]
    public class InscripcionPagoController : ControllerBase
    {
        private readonly KalumDBContext DbContext;
        private readonly ILogger<InscripcionPagoController> Logger;
        private readonly IMapper Mapper;
        public InscripcionPagoController(KalumDBContext _DbContext, ILogger<InscripcionPagoController> _Logger, IMapper _Mapper)
        {
            this.DbContext = _DbContext;
            this.Logger = _Logger;
            this.Mapper = _Mapper;            
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<InscripcionPagoListDTO>>> Get()
        {
            List<InscripcionPago> pagos = null;
            Logger.LogDebug("Iniciando proceso de consulta de pagos de inscripción");
            pagos = await DbContext.InscripcionPago.Include(i => i.Aspirante).ToListAsync();
            if(pagos == null)
            {
                Logger.LogWarning("No existen pagos de inscripción");
                return new NoContentResult();
            }
            List<InscripcionPagoListDTO> pagosInscipciones = Mapper.Map<List<InscripcionPagoListDTO>>(pagos);
            Logger.LogInformation("Se ejecuto la petición de forma exitosa");
            return Ok(pagosInscipciones);
        }

        [HttpGet("{boletaPago}/{noExpediente}/{anio}", Name="GetPago")]
        public async Task<ActionResult<InscripcionPagoListDTO>> GetPago(string boletaPago, string noExpediente, string anio)
        {
            Logger.LogDebug($"Iniciando busqueda de pago con boleta {boletaPago}, no expediente {noExpediente} y anio {anio}");
            var pago = await DbContext.InscripcionPago.Include(i => i.Aspirante).FirstOrDefaultAsync(i => i.BoletaPago == boletaPago && i.NoExpediente == noExpediente && i.Anio == anio);
            if(pago == null)
            {
                Logger.LogWarning($"No existe pago con boleta {boletaPago}, no expediente {noExpediente} y anio {anio}");
                return new NoContentResult();
            }
            InscripcionPagoListDTO pagoInscripcion = Mapper.Map<InscripcionPagoListDTO>(pago);
            Logger.LogInformation("Finalizando el proceso de busqueda de forma exitosa");
            return Ok(pagoInscripcion);
        }

        [HttpGet("page/{page}")]
        public async Task<ActionResult<IEnumerable<InscripcionListDTO>>> GetPaginacion(int page)
        {
            Logger.LogDebug("Iniciando paginacion pagos");
            var queryable = DbContext.InscripcionPago.Include(i => i.Aspirante).AsQueryable();
            var paginacion = new HttpResponsePaginacion<InscripcionPago>(queryable, page);
            if(paginacion.Content == null && paginacion.Content.Count == 0)
            {
                Logger.LogWarning("No existen registros para paginar");
                return NoContent();
            }
            List<InscripcionListDTO> pagosInscripcion = Mapper.Map<List<InscripcionListDTO>>(paginacion.Content);
            Logger.LogInformation("Finalizando proceso de paginacion pagos");
            return Ok(pagosInscripcion);
        }

        [HttpPost]
        public async Task<ActionResult<InscripcionPagoListDTO>> Post([FromBody] InscripcionPago value)
        {
            Logger.LogDebug("Iniciando proceso de agregar nuevo pago");
            Aspirante aspirante = await DbContext.Aspirante.FirstOrDefaultAsync(a => a.NoExpediente == value.NoExpediente);
            if (aspirante == null)
            {
                Logger.LogWarning($"No existe el aspirante con NoExpediente {value.NoExpediente}");
                return BadRequest();
            }
            await DbContext.InscripcionPago.AddAsync(value);
            await DbContext.SaveChangesAsync();
            Logger.LogInformation("Finalizando el proceso de agregar un pago");
            InscripcionPagoListDTO pago = Mapper.Map<InscripcionPagoListDTO>(value);
            return new CreatedAtRouteResult("GetPago", new {boletaPago = pago.BoletaPago, noExpediente = pago.NoExpediente, anio = pago.Anio}, pago);
        }

        [HttpDelete("{boletaPago}/{noExpediente}/{anio}")]
        public async Task<ActionResult<InscripcionPago>> Delete(string boletaPago, string noExpediente, string anio)
        {
            Logger.LogDebug("Iniciando proceso de eliminación");
            InscripcionPago pago = await DbContext.InscripcionPago.FirstOrDefaultAsync(i => i.BoletaPago == boletaPago && i.NoExpediente == noExpediente && i.Anio==anio);
            if(pago == null)
            {
                Logger.LogWarning($"No se encontro pago con la boleta {boletaPago}, no expediente {noExpediente} y anio {anio}");
                return NotFound();     
            }
            else
            {
                DbContext.InscripcionPago.Remove(pago);
                await DbContext.SaveChangesAsync();
                Logger.LogWarning($"Se ha eliminado correctamente el pago con la boleta {boletaPago}, no expediente {noExpediente} y anio {anio}");
                return pago;
            }
        }

        [HttpPut("{boletaPago}/{noExpediente}/{anio}")]
        public async Task<ActionResult> Put(string boletaPago, string noExpediente, string anio, [FromBody] InscripcionPago value)
        {
            Logger.LogDebug($"Iniciando el proceso de actualización de pago con boleta {boletaPago}, no expediente {noExpediente} y anio {anio}");
            InscripcionPago pago = await DbContext.InscripcionPago.FirstOrDefaultAsync(i => i.BoletaPago == boletaPago && i.NoExpediente == noExpediente && i.Anio == anio);
            if(pago == null)
            {
                Logger.LogWarning($"No se encontro ningun pago con boleta {boletaPago}, no expediente {noExpediente} y anio {anio}");
                return BadRequest();
            }
            else
            {
                pago.FechaPago = value.FechaPago;
                pago.Monto = value.Monto;
                DbContext.Entry(pago).State = EntityState.Modified;
                await DbContext.SaveChangesAsync();
                Logger.LogInformation("Los datos han sido actualizados correctamente");
                return NoContent();
            }
        }
    }
}