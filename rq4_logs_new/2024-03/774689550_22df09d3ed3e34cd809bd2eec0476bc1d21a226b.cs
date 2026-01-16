using DataAccess.CRUD;
using DTO;
using Microsoft.AspNetCore.Mvc;

namespace WebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    /*ATRIBUTOS: [Route("api/[controller]")] especifica la ruta base de la API,
     * y [ApiController] indica que la clase es un controlador de API.*/

    public class AppointmentController : ControllerBase
    {

        [HttpPost]
        [Route("Create")]
        public ActionResult Create(Appointment appointment)
        {
            try
            {
                var aptc = new AppointmentCrudFactory();
                aptc.Create(appointment);
                return Ok(appointment);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet]
        [Route("RetrieveById")]
        public ActionResult RetrieveById(int id)
        {
            try
            {
                var aptc = new AppointmentCrudFactory();
                var apptId = new Appointment { Id = id };
                var appointment = aptc.RetrieveById<Appointment>(id);
                return Ok(appointment);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet]
        [Route("RetrieveAll")]
        public ActionResult RetrieveAll()
        {
            try
            {
                var aptc = new AppointmentCrudFactory();
                var lastAppointments = aptc.RetrieveAll<Appointment>();
                return Ok(lastAppointments);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpPut]
        [Route("Update")]
        public ActionResult Update(Appointment appointment)
        {
            try
            {
                var aptc = new AppointmentCrudFactory();
                aptc.Update(appointment);
                return Ok("Appointment updated successfully");
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpDelete]
        [Route("Delete")]
        public ActionResult Delete(Appointment appointment)
        {
            try
            {
                var aptc = new AppointmentCrudFactory();
                aptc.Delete(appointment);
                return Ok("Appointment deleted successfully");
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }


    }
}