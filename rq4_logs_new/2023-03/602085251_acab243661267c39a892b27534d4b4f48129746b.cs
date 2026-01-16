using comp1640_dotnet.Data;
using comp1640_dotnet.DTOs.Requests;
using comp1640_dotnet.DTOs.Responses;
using comp1640_dotnet.Models;
using comp1640_dotnet.Repositories.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace comp1640_dotnet.Controllers
{
	[Route("[controller]")]
	[ApiController]
	public class AcademicYearsController : ControllerBase
	{
		private readonly IAcademicYearRepository academicYearRepos;

		public AcademicYearsController(IAcademicYearRepository _academicYearRepos)
		{
			this.academicYearRepos = _academicYearRepos;
		}

		[HttpGet]
		public async Task<ActionResult<IEnumerable<AcademicYear>>> GetAcademicYears()
		{
			var result = await academicYearRepos.GetAcademicYears();
			return Ok(result);
		}

		[HttpGet("{id}")]
		public async Task<ActionResult<AcademicYearResponse>> GetAcademicYear(string id, int pageIndex = 1)
		{
			var result = await academicYearRepos.GetAcademicYear(id, pageIndex);
			if(result == null)
			{
				return BadRequest("Academic Year not found");
			}
			return Ok(result);
		}

		[HttpPost]
		public async Task<ActionResult<AcademicYearResponse>> CreateAcademicYear(AcademicYearRequest academicYear)
		{
			var result = await academicYearRepos.CreateAcademicYear(academicYear);
			return Ok(result);
		}

		[HttpDelete("{id}")]
		public async Task<ActionResult<AcademicYear>> RemoveAcademicYear(string id)
		{
			var result = await academicYearRepos.RemoveAcademicYear(id);
			if(result == null)
			{
				return BadRequest("Academic Year not found");
			}
			return Ok("Delete successful academic year");
		}

		[HttpPut("{id}")]
		public async Task<ActionResult<AcademicYearResponse>> UpdateAcademicYear(string id, AcademicYearRequest academicYear)
		{
			var result = await academicYearRepos.UpdateAcademicYear(id, academicYear);
			if (result == null)
			{
				return BadRequest("AcademicYear not found");
			}
			return Ok("Update successful academic year");
		}
	}
}