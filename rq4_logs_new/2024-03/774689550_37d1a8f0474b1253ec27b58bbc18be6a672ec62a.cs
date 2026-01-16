using CoreApp;
using DTO;
using Microsoft.AspNetCore.Mvc;

namespace WebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PatientController : ControllerBase
    {
        //Siempre vamos a retornar 2 respuestas
        //200 --> OK
        //500 --> Internal server Error
        //Los retrieve trabajan con el verbo get de http
        [HttpGet]
        [Route("RetrieveAll")]
        public ActionResult RetrieveAll()
        {
            try
            {
                var pm = new PatientManager();
                var patientList = pm.RetrieveAll();
                return Ok(patientList);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }

        }

        [HttpGet]
        [Route("RetrieveById")]
        public ActionResult RetrieveById(int patientId)
        {
            try
            {
                var pm = new PatientManager();
                var patient = pm.RetrieveById(patientId);

                if (patient != null)
                {
                    return Ok(patient);
                }
                else
                {
                    return NotFound("Patient not found");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }



        [HttpPost]
        [Route("Create")]
        public ActionResult Create(Patient patient)
        {
            try
            {
                var pm = new PatientManager();
                pm.Create(patient);
                return Ok(patient);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }



        }

        [HttpPut]
        [Route("Update")]
        public ActionResult Update(Patient patient)
        {
            try
            {
                var pm = new PatientManager();
                pm.Update(patient);
                return Ok(patient);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }

        }



        [HttpDelete]
        [Route("Delete")]
        public ActionResult Delete(Patient patient)
        {
            try
            {
                var pm = new PatientManager();
                pm.Delete(patient);
                return Ok(patient);


            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }
    }
}