using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Mvc;
using PruebaEncriptacionNetCore.Services;
using System;
using System.Security.Policy;
using System.Text;

namespace PruebaEncriptacionNetCore.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PruebasEncriptacionController : ControllerBase
    {
        private IDataProtector _dataProtector;
        private ITimeLimitedDataProtector _dataProtectorTiempoLimitado;
        private HashService _hashService;

        public PruebasEncriptacionController(IDataProtectionProvider dataProtectionProvider, HashService hashService)
        {
            //Esta encriptacion usa una clave interna que net core gestiona (Segun si esta en azure o en IIS...). Hay forma para controlar donde se guarda esa clave por ejemplo si se usa microservicios y no
            //se tiene la clave en un almacen comun cada microservicio encriptaria de forma distinta.
            //Recomendado obtener la clave de un repositorio seguro, secretos o variable de entorno.
            _dataProtector = dataProtectionProvider.CreateProtector("Clave");
            _dataProtectorTiempoLimitado = dataProtectionProvider.CreateProtector("Clave").ToTimeLimitedDataProtector();
            
            _hashService = hashService;
        }

        [HttpGet("encriptar/{Texto}")]
        public ActionResult<object> GetEncriptado(string texto)
        {
            return Ok(_dataProtector.Protect(texto));
        }

        [HttpGet("encriptarTiempo/{Texto}")]
        public ActionResult<object> GetEncriptadoTiempo(string texto)
        {
            return Ok(_dataProtectorTiempoLimitado.Protect(texto, TimeSpan.FromSeconds(5)));
        }

        [HttpGet("desencriptar/{TextoEncriptado}")]
        public ActionResult<object> GetDesencriptado(string textoEncriptado)
        {
            return Ok(_dataProtector.Unprotect(textoEncriptado));
        }

        [HttpGet("desencriptarTiempo/{TextoEncriptado}")]
        public ActionResult<object> GetDesencriptadoTiempo(string textoEncriptado)
        {
            return Ok(_dataProtectorTiempoLimitado.Unprotect(textoEncriptado));
        }

        [HttpGet("hash/{TextoEncriptado}/{Salt}")]
        public ActionResult<object> GetObtenerHash(string textoEncriptado,string salt)
        {
            byte[] bytesSalt = Encoding.ASCII.GetBytes(salt);
            return Ok(_hashService.Hash(textoEncriptado,bytesSalt));
        }

    }
}