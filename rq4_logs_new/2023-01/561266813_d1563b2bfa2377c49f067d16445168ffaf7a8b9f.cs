using System;
using Domain.Models;
using Domain.Services;
using ITproject.Views;
using Microsoft.AspNetCore.Mvc;

namespace ITproject.Controllers
{
    [ApiController]
    [Route("doctor")]
    public class DoctorController : ControllerBase
    {
        private readonly DoctorRepository _service;
        public DoctorController(DoctorRepository service)
        {
            _service = service;
        }

        [HttpGet("get_all")]
        public ActionResult<List<DoctorSearchView>> GetAllDoctors()
        {
            var temp = _service.GetAll();

            List<DoctorSearchView> arr_docs = new List<DoctorSearchView>();

            foreach (var doc in temp.Value)
            {
                var result = new DoctorSearchView
                {
                    DoctorId = doc.Id,
                    Name = doc.FullName,
                    Specialization = doc.Specialization
                };
                arr_docs.Add(result);
            }


            if (!temp.Success)
                return Problem(statusCode: 404, detail: temp.Error);


            return Ok(arr_docs);
        }

        [HttpGet("find")]
        public ActionResult<DoctorSearchView> FindDoctor(int id)
        {
            var temp = _service.GetById(id);

            if (!temp.Success)
                return Problem(statusCode: 404, detail: temp.Error);

            var doc = temp.Value;
            return Ok(new DoctorSearchView
            {
                DoctorId = doc.Id,
                Name = doc.FullName,
                Specialization = doc.Specialization
            });
        }

        [HttpGet("spec")]
        public ActionResult<List<DoctorSearchView>> GetBySpec(Specialization specialization)
        {
            var arr = _service.GetBySpec(specialization);

            List<DoctorSearchView> arr_docs = new List<DoctorSearchView>();

            foreach (var doc in arr.Value)
            {
                var result = new DoctorSearchView
                {
                    DoctorId = doc.Id,
                    Name = doc.FullName,
                    Specialization = doc.Specialization
                };
                arr_docs.Add(result);
            }

            if (!arr.Success)
                return Problem(statusCode: 404, detail: arr.Error);

            return Ok(arr_docs);
        }

        [HttpPost("create")]
        public ActionResult<DoctorSearchView> CreateDoctor(string name, Specialization specialization)
        {
            Doctor doc = new(0, name, specialization);

            var temp = _service.CreateDoctor(doc);

            if (!temp.Success)
                return Problem(statusCode: 404, detail: temp.Error);

            return Ok(new DoctorSearchView
            {
                DoctorId = doc.Id,
                Name = doc.FullName,
                Specialization = doc.Specialization
            });
        }

        [HttpDelete("delete")]
        public ActionResult<DoctorSearchView> DeleteDoctor(int id)
        {
            var temp = _service.DeleteDoctor(id);

            if (!temp.Success)
                return Problem(statusCode: 404, detail: temp.Error);

            return Ok();
        }






    }
}
