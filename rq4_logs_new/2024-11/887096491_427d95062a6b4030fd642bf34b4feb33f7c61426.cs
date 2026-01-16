using MediatR;
using Microsoft.AspNetCore.Mvc;
using WolfDen.Application.Requests.Queries.Attendence.AttendenceLog;
using static System.Runtime.InteropServices.JavaScript.JSType;
using WolfDen.Domain.Entity;


namespace WolfDen.API.Controllers.Attendence
{
    [Route("api/[controller]")]
    [ApiController]
    public class AttendenceLog : ControllerBase
    {
        private readonly IMediator _mediator;

        public AttendenceLog(IMediator mediator)
        {
            _mediator = mediator;
        }

        [HttpGet("{employeeId}/{date}")]
        public async Task<IActionResult> GetAttendenceLog(int employeeId, DateOnly date)
        {
            AttendenceLogQuery AttendenceLog = new AttendenceLogQuery();
            AttendenceLog.EmployeeId = employeeId;
            AttendenceLog.Date = date;
            var AttendenceList = await _mediator.Send(AttendenceLog);

            if (AttendenceList==null)
                return NotFound("No Attendence Log found");
            return Ok(AttendenceList);
        }

        [HttpGet("{employeeId}/{date}/downloadPdf")]
        public async Task<IResult> GeneratePdf(int employeeId,DateOnly date)
        {

            AttendenceLogQuery AttendenceLog = new AttendenceLogQuery();
            AttendenceLog.EmployeeId = employeeId;
            AttendenceLog.Date = date;
            var AttendenceList = await _mediator.Send(AttendenceLog);
            var document = _pdfService.CreateDocument(AttendenceList);


            var pdf = document.GeneratePdf();
            return Results.File(pdf, "application/pdf", "customer-report.pdf");
        }

    }
}