using MediatR;
using Microsoft.AspNetCore.Mvc;
using QuestPDF.Fluent;
using WolfDen.Application.Requests.DTOs.Attendence;
using WolfDen.Application.Requests.Queries.Attendence.AttendanceSummary;
using WolfDen.Application.Requests.Queries.Attendence.AttendenceLog;


namespace WolfDen.API.Controllers.Attendence
{
    [Route("api/[controller]")]
    [ApiController]
    public class AttendenceLog : ControllerBase
    {
        private readonly IMediator _mediator;
        private readonly PdfService _pdfService;

        public AttendenceLog(IMediator mediator, PdfService pdfService)
        {
            _mediator = mediator;
            _pdfService = pdfService;
        }

        [HttpGet("employee/{employeeId}/monthly")]
        public async Task<AttendanceSummaryDTO> GetMonthlyAttendance(int employeeId, [FromQuery] int year, [FromQuery] int month)
        {

            AttendanceSummaryQuery query = new AttendanceSummaryQuery();
            query.EmployeeId = employeeId;
            query.Year = year;
            query.Month = month;
            return await _mediator.Send(query);
        }

        //    try
        //    {
        //        // Create the query object
        //        var query = new AttendanceSummaryQuery(employeeId, year, month);

        //        // Use the query handler to get the summary
        //        var summary = await AttendanceSummaryQueryHandler.Handle(query);

        //        // Return the DTO as a response
        //        return Ok(summary);
        //    }
        //    catch (ArgumentException ex)
        //    {
        //        // Handle invalid year or month
        //        return BadRequest(ex.Message);
        //    }
        //    catch (Exception ex)
        //    {
        //        // Handle general server error
        //        return StatusCode(500, "Internal server error");
        //    }
        //}

        //[HttpGet("{employeeId}/{date}")]
        //public async Task<IActionResult> GetAttendenceLog(int employeeId, DateOnly date)
        //{
        //    AttendenceLogQuery AttendenceLog = new AttendenceLogQuery();
        //    AttendenceLog.EmployeeId = employeeId;
        //    AttendenceLog.Date = date;
        //    var AttendenceList = await _mediator.Send(AttendenceLog);

        //    if (AttendenceList == null)
        //        return NotFound("No Attendence Log found");
        //    return Ok(AttendenceList);
        //}

        //[HttpGet("{employeeId}/{date}/downloadPdf")]
        //public async Task<IResult> GeneratePdf(int employeeId, DateOnly date)
        //{

        //    AttendenceLogQuery AttendenceLog = new AttendenceLogQuery();
        //    AttendenceLog.EmployeeId = employeeId;
        //    AttendenceLog.Date = date;
        //    var AttendenceList = await _mediator.Send(AttendenceLog);
        //    var document = _pdfService.CreateDocument(AttendenceList);


        //    var pdf = document.GeneratePdf();
        //    return Results.File(pdf, "application/pdf", "customer-report.pdf");
        //}


        //[HttpGet("{employeeId}/{week}"]
        //public async Task<IActionResult> GetWeeklyAttendence(int employeeId, string week)
        //{
        //    AttendenceLogQuery AttendenceLog = new AttendenceLogQuery();
        //    AttendenceLog.EmployeeId = employeeId;
        //    AttendenceLog.Date = date;
        //    var AttendenceList = await _mediator.Send(AttendenceLog);

        //    if (AttendenceList == null)
        //        return NotFound("No Attendence Log found");
        //    return Ok(AttendenceList);
        //}



    }
}