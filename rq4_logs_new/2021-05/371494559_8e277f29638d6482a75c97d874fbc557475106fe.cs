using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using RestApi_Web_Labs_2.Interfaces;
using RestApi_Web_Labs_2.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;


namespace RestApi_Web_Labs_2.Controllers
{

    [Route("")]
    [Route("Home")]
    //[Route("api/[controller]")]
    [ApiController]
    public class HomeController : ControllerBase
    {
        private IRepairService RepairService { get; set; }
        private IBaseRepository<PhotoModel> Photos { get; set; }

        public HomeController(IRepairService repairService, IBaseRepository<PhotoModel> Photo)
        {
            RepairService = repairService;
            Photos = Photo;
        }
        [HttpGet]

        public JsonResult Get()
        {
            return new JsonResult(Photos.GetAll());
        }
        [Route("Home/GetName/{name?}")]
        [HttpGet]
        public JsonResult GetName(string name)
        {
            return new JsonResult(Photos.Get(name));
        }
        [Route("Home/Count/{count?}")]
        [HttpGet]
        public JsonResult Count(int count)
        {
            
            var list = Photos.GetAll();
            if (count < list.Count())    return new JsonResult(list.GetRange(0, count));
            return new JsonResult(list);
        }
        [Route("Home/Post")]
        [HttpPost]
        public JsonResult Post(string text)
        {
            RepairService.Work();
            return new JsonResult("Work was successfully done");
        }
        [Route("Home/Put")]
        [HttpPut]
        public JsonResult Put(Guid id, string Name,string Description)
        {
            bool success = true;
            var document = Photos.Get(id);
            try
            {
                if (document != null)
                {
                    document.Name = Name;
                    document.Description = Description;
                    document = Photos.Update(document);
                }
                else
                {
                    success = false;
                }
            }
            catch (Exception)
            {
                success = false;
            }

            return success ? new JsonResult($"Update successful {document.Id}  {document.Name}") : new JsonResult("Update was not successful");
        }
        [Route("Home/Delete")]
        [HttpDelete]
        public JsonResult Delete(Guid id)
        {
            bool success = true;
            var photo = Photos.Get(id);

            try
            {
                if (photo != null)
                {
                    Photos.Delete(photo.Id);
                }
                else
                {
                    success = false;
                }
            }
            catch (Exception)
            {
                success = false;
            }

            return success ? new JsonResult("Delete successful") : new JsonResult("Delete was not successful");
        }
    }
}