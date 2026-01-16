using System.Text;
using System.Text.Json;
using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using WebApiKalum.Dtos;
using WebApiKalum.Entities;

namespace WebApiKalum.Controllers
{
    [ApiController]
    [Route("v1/KalumManagement/Inscripcion")]
    public class InscripcionController : ControllerBase
    {
        private readonly KalumDBContext DbContext;
        private readonly ILogger<InscripcionController> Logger;
        private readonly IMapper Mapper;

        public InscripcionController(KalumDBContext _DbContext, ILogger<InscripcionController> _Logger)
        {
            this.DbContext = _DbContext;
            this.Logger = _Logger;            
        }

        [HttpPost("Enrollments")]
        public async Task<ActionResult<ResponseEnrollmentDTO>> EnrollmentCreateAsync([FromBody] EnrollmentDTO value)
        {
            Aspirante aspirante = await DbContext.Aspirante.FirstOrDefaultAsync(a => a.NoExpediente == value.NoExpediente);
            if(aspirante == null)
            {
                return NoContent();
            }
            CarreraTecnica carreraTecnica = await DbContext.CarreraTecnica.FirstOrDefaultAsync(ct => ct.CarreraId == value.CarreraId);
            if(carreraTecnica == null)
            {
                return NoContent();
            }            
            bool respuesta = await CrearSolicitudAsync(value);
            if(respuesta == true)
            {
                ResponseEnrollmentDTO response = new ResponseEnrollmentDTO();
                response.HttpStatus = 201;
                response.Message = "El proceso de inscripción se ha realizado con exito";
                return Ok(response);
            }
            else
            {
                return StatusCode(503, value);
            }
        }

        private async Task<bool> CrearSolicitudAsync(EnrollmentDTO value)
        {
            bool proceso = false;
            ConnectionFactory factory = new ConnectionFactory();
            IConnection conexion = null;
            IModel channel = null;
            factory.HostName = "localhost";
            factory.VirtualHost = "/";
            factory.Port = 5672;
            factory.UserName = "guest";
            factory.Password = "guest";
            try
            {
                conexion = factory.CreateConnection();
                channel = conexion.CreateModel();
                channel.BasicPublish("kalum.exchange.enrollment","",null,Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value))) ;
                proceso = true ;
            } 
            catch(Exception e)
            {
                Logger.LogError(e.Message);
            }
            finally
            {
                channel.Close();
                conexion.Close();
            }
            return proceso;
        }
    }
}