using CandidateService.Service;
using InterviewService.Service;
using Microsoft.AspNetCore.Mvc;

namespace InterviewService.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class InterviewController: ControllerBase
    {
        IInterviewService InterviewService { get; set; }
        public InterviewController(IInterviewService interviewService)
        {
            this.InterviewService = interviewService;
        }

        
    }
}