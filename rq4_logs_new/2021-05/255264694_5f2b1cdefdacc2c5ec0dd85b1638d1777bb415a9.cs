using System;
using System.Threading.Tasks;
using insightcampus_api.Dao;
using insightcampus_api.Data;
using insightcampus_api.Model;
using Microsoft.AspNetCore.Mvc;

namespace insightcampus_api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ClassStudentController : ControllerBase
    {
        private readonly ClassStudentInterface _classstudent;

        public ClassStudentController(ClassStudentInterface classstudent)
        {
            _classstudent = classstudent;
        }

        [HttpGet("{class_seq}/{size}/{pageNumber}")]
        public async Task<ActionResult<DataTableOutDto>> Get(int class_seq, int size, int pageNumber)
        {
            DataTableInputDto dataTableInputDto = new DataTableInputDto();
            dataTableInputDto.size = size;
            dataTableInputDto.pageNumber = pageNumber;
            dataTableInputDto.class_seq = class_seq;

            return await _classstudent.Select(class_seq, dataTableInputDto);
        }

    }

}