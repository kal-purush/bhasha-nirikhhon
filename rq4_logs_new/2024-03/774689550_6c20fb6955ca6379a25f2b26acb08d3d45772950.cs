using CoreApp;
using DTO;
using Microsoft.AspNetCore.Mvc;

namespace WebAPI.Controllers
{ 
        [Route("api/[controller]")]
        [ApiController]

        public class DoctorController: ControllerBase
        {
        [HttpGet]
        [Route("RetrieveAll")]
        public ActionResult RetrieveAll()
        {
            try
            {
                var dm = new DoctorManager();
                var doctorList = dm.RetrieveAll();
                return Ok(doctorList);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }

        }

        [HttpGet]
        [Route("RetrieveById")]
        public ActionResult RetrieveById(int Id)
        {
            try
            {
                var dm = new DoctorManager();
                var doctor = dm.RetrieveById(Id);

                if (doctor != null)
                {
                    return Ok(doctor);
                }
                else
                {
                    return NotFound("Doctor not found");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }



        [HttpPost]
        [Route("Create")]
        public ActionResult Create(Doctor doctor)
        {
            try
            {
                var dm = new DoctorManager();
                dm.Create(doctor);
                return Ok(doctor);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpPut]
        [Route("Update")]
        public ActionResult Update(Doctor doctor)
        {
            try
            {
                var dm = new DoctorManager();
                dm.Update(doctor);
                return Ok(doctor);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }



        [HttpDelete]
        [Route("Delete")]
        public ActionResult Delete(Doctor doctor)
        {
            try
            {
                var dm = new DoctorManager();
                dm.Delete(doctor);
                return Ok(doctor);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

    }
}