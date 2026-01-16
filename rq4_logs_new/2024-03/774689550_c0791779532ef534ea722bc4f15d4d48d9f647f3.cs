using CoreApp;
using DTO;
using Microsoft.AspNetCore.Mvc;

namespace WebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ServiceController : Controller
    {
        [HttpGet]
        [Route("RetrieveAll")]
        public ActionResult RetrieveAll()
        {
            try
            {
                var sm = new ServiceManager();
                var srvList = sm.RetrieveAll<Service>();
                return Ok(srvList);
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
                var sm = new ServiceManager();
                var service = sm.RetrieveById(Id);

                if (service != null) {
                    return Ok(service);
                }
                else {
                    return NotFound("service not found");
                }
            } catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }


        [HttpPost]
        [Route("Create")]
        public ActionResult Create(Service service)
        {
            try
            {
                var sm = new ServiceManager();
                sm.Create(service);
                return Ok(service);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpPut]
        [Route("Update")]
        public ActionResult Update(Service service)
        {
            try
            {
                var sm = new ServiceManager();
                sm.Update(service);
                return Ok(service);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }



        [HttpDelete]
        [Route("Delete")]
        public ActionResult Delete(Service service)
        {
            try
            {
                var sm = new ServiceManager();
                sm.Delete(service);
                return Ok(service);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }
    }
}