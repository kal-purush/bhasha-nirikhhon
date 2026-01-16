using AdmissionSystem2.Entites;
using AdmissionSystem2.Helpers;
using AdmissionSystem2.Models;
using AdmissionSystem2.Services;
using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AdmissionSystem2.Controllers
{
    [Route("api/admin")]
    public class AdminController : Controller
    {
        private IAdminRepo _AdminRepo;
        private readonly IMapper _Mapper;
        public AdminController(IAdminRepo AdminRepo, IMapper Mapper)
        {
            _AdminRepo = AdminRepo;
            _Mapper = Mapper;
        }
        public IActionResult Index()
        {
            return View();
        }

        [HttpGet("Application")]
        public IActionResult GetApplication(ResourceParameters resourceParameters)
        {
            var Applications = _AdminRepo.GetApplication(resourceParameters);

            var ApplicationToReturn = _Mapper.Map<IEnumerable<ApplicationDto>>(Applications);

            return Ok(ApplicationToReturn);
        }
    }
}