using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using WebApiKalum.Dtos;
using WebApiKalum.Entities;
using WebApiKalum.Utilities;


namespace WebApiKalum.Controllers
{
    [ApiController]
    [Route("v1/KalumManagement/[controller]")]
    public class CuentaxCobrarController : ControllerBase
    {
        private readonly KalumDBContext DbContext;
        private readonly ILogger<CuentaxCobrarController> Logger;
        private readonly IMapper Mapper;

        public CuentaxCobrarController(KalumDBContext dbContext, ILogger<CuentaxCobrarController> _Logger, IMapper _Mapper)
        {
            this.DbContext = dbContext;
            this.Logger = _Logger;
            this.Mapper = _Mapper;
        }
        
        [HttpGet]
        public async Task<ActionResult<IEnumerable<CuentaxCobrarListDTO>>> Get()
        {
            List<CuentaxCobrar> cuentasxCobrar = null;
            Logger.LogDebug("Iniciando proceso de consulta de cuentas por cobrar");
            cuentasxCobrar = await DbContext.CuentaxCobrar.Include(cxc => cxc.Alumno).ToListAsync();
            if(cuentasxCobrar == null || cuentasxCobrar.Count==0)
            {
                Logger.LogWarning("No existen cuentas por cobrar");
                return new NoContentResult();
            }
            List<CuentaxCobrarListDTO> cuentas = Mapper.Map<List<CuentaxCobrarListDTO>>(cuentasxCobrar);
            Logger.LogInformation("Se ejecuto la petición de forma exitosa");
            return Ok(cuentas);
        }

        [HttpGet("{carne}", Name = "GetCuentaxCobrar")]
        public async Task<ActionResult<CuentaxCobrarListDTO>> GetCuentaPorCobrar(string carne)
        {
            List<CuentaxCobrar> cuenta = null;
            Logger.LogDebug("Iniciando el proceso de busqueda con el carne " + carne);
            cuenta = await DbContext.CuentaxCobrar.Include(cxc => cxc.Alumno).Where(cxc => cxc.Carne == carne).ToListAsync();
            if(cuenta == null)
            {
                Logger.LogWarning("No existe cuenta por cobrar con el carne " + carne);
                return new NoContentResult();
            }
            List<CuentaxCobrarListDTO> cuentaxCobrar = Mapper.Map<List<CuentaxCobrarListDTO>>(cuenta);
            Logger.LogInformation("Finalizando el proceso de busqueda de forma exitosa");
            return Ok(cuentaxCobrar);
        }

        [HttpGet("page/{page}")]
        public async Task<ActionResult<IEnumerable<CuentaxCobrarListDTO>>> GetPaginacion(int page)
        {
            Logger.LogDebug("Iniciando paginacion cuenta por cobrar");
            var queryable = DbContext.CuentaxCobrar.Include(cxc => cxc.Alumno).AsQueryable();
            var paginacion = new HttpResponsePaginacion<CuentaxCobrar>(queryable, page);
            if(paginacion.Content == null && paginacion.Content.Count == 0) 
            {
                Logger.LogWarning("No existen registros para paginar");
                return NoContent();
            }
            List<CuentaxCobrarListDTO> cuentaxCobrar = Mapper.Map<List<CuentaxCobrarListDTO>>(paginacion.Content);
            Logger.LogInformation("Finalizando proceso de paginacion alumnos");
            return Ok(cuentaxCobrar);            
        }

        [HttpPost]
        public async Task<ActionResult<CuentaxCobrar>> Post([FromBody] CuentaxCobrarCreateDTO value)
        {
            Logger.LogDebug("Iniciando proceso de agregar cuenta por cobrar");
            Alumno alumno = await DbContext.Alumno.FirstOrDefaultAsync(a => a.Carne == value.Carne);
            if(alumno == null)
            {
                Logger.LogInformation($"No existe alumno con carne {value.Carne}");
                return BadRequest();
            }
            Cargo cargo = await DbContext.Cargo.FirstOrDefaultAsync(c => c.CargoId == value.CargoId);
            if(cargo == null)
            {
                Logger.LogInformation($"No existe cargo con id {value.CargoId}");
                return BadRequest();
            }
            CuentaxCobrar nuevo = Mapper.Map<CuentaxCobrar>(value);
            await DbContext.CuentaxCobrar.AddAsync(nuevo);
            await DbContext.SaveChangesAsync();
            Logger.LogInformation("Finalizando el proceso de agregar registro a cuenta por cobrar");
            return new CreatedAtRouteResult("GetCuentaxCobrar", new {carne = nuevo.Carne}, value);
        }
        
        [HttpDelete("{nombreCargo}/{anio}/{carne}")]
        public async Task<ActionResult<CuentaxCobrar>> Delete(string nombreCargo,string anio,string carne)
        {
            Logger.LogDebug("Iniciando proceso de eliminación");
            Alumno alumno = await DbContext.Alumno.FirstOrDefaultAsync(a => a.Carne == carne);
            if(alumno == null)
            {
                Logger.LogWarning($"No se encontro ningun alumno con el carne {carne}");
                return NotFound();
            }
            CuentaxCobrar cuentaxCobrar = await DbContext.CuentaxCobrar.FirstOrDefaultAsync(cxc => cxc.NombreCargo == nombreCargo 
                                                                                            && cxc.Anio == anio
                                                                                            && cxc.Carne == carne);
            if(cuentaxCobrar == null)
            {
                Logger.LogWarning($"No existe cuenta por cobrar con nombre de cargo {nombreCargo}, anio {anio} y carne {carne}");
                return BadRequest();
            }
            else
            {
                DbContext.CuentaxCobrar.Remove(cuentaxCobrar);
                await DbContext.SaveChangesAsync();
                Logger.LogInformation($"Se ha eliminado correctamente la cuenta por cobrar con nombre de cargo {nombreCargo}, anio {anio} y carne {carne}");
                return cuentaxCobrar;
            }
        }

        [HttpPut("{nombreCargo}/{anio}/{carne}")]
        public async Task<ActionResult> Put(string nombreCargo,string anio,string carne,[FromBody] CuentaxCobrar value)
        {
            Logger.LogDebug("Iniciando el proceso de actualización cuenta corriente");
            CuentaxCobrar cuentaxCobrar = await DbContext.CuentaxCobrar.FirstOrDefaultAsync(cxc => cxc.NombreCargo == nombreCargo 
                                                                                            && cxc.Anio == anio
                                                                                            && cxc.Carne == carne);
            if(cuentaxCobrar == null)
            {
                Logger.LogWarning("No se encontro la cuenta por cobrar especificada");
                return NotFound();
            }
            else
            {
                cuentaxCobrar.NombreCargo = value.NombreCargo;
                cuentaxCobrar.Anio = value.Anio;
                cuentaxCobrar.Carne = value.Carne;
                cuentaxCobrar.CargoId = value.CargoId;
                cuentaxCobrar.Descripcion = value.Descripcion;
                cuentaxCobrar.FechaCargo = value.FechaCargo;
                cuentaxCobrar.FechaAplica = value.FechaAplica;
                cuentaxCobrar.Monto = value.Monto;
                cuentaxCobrar.Mora = value.Mora;
                cuentaxCobrar.Descuento = value.Descuento;
                DbContext.Entry(cuentaxCobrar).State = EntityState.Modified;
                await DbContext.SaveChangesAsync();
                Logger.LogInformation("Los datos han sido actualizados correctamente");
                return NoContent();
            }
        }
    }
}