using LoginApi.Dto;
using LoginApi.IRepositories;
using LoginApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace LoginApi.Controllers
{
    [ApiController]
    [Route("api/[controller]/[action]")]
    public class CompanyDetailsController : ControllerBase
    {
        public readonly ICompanyRepository _companyRepository;
        public CompanyDetailsController(ICompanyRepository companyReopsitory)
        {
            _companyRepository = companyReopsitory;
        }


        [HttpPost]
        public async Task<IActionResult> CreateCompanyandPerson([FromBody] CompanyFullDetails newcompanyandperson)
        {
            await _companyRepository.createcompanyandpersondetails(newcompanyandperson);
            return Ok();
        }


        [HttpPost]
        public async Task<IActionResult> CreatePerson([FromBody] PersonDetails newperson)
        {
            await _companyRepository.createperson(newperson);
            return Ok();
        }



        [HttpGet]
        public List<Companydetails> GetCompanydetails()
        {
            return _companyRepository.getcompany();
        }


        [HttpGet]
        public List<PersonDetails> GetPersonDetails( int id)
        {
            return _companyRepository.getperson(id);
        }

    }
}