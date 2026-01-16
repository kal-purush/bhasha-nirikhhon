using MarisDoodleLibrary.Contracts.Routines;
using MarisDoodleLibrary.Models;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

namespace DoodleWebMvc.Controllers
{
    public class ResultController : Controller
    {
        private readonly IResultRoutine _resultRoutine;

        public ResultController(IResultRoutine resultRoutine)
        {
            _resultRoutine = resultRoutine;
        }

        public async Task<IActionResult> Index(int id)
        {
            PollVotingResultModel displayModel = await _resultRoutine.GetVotingResults(id);

            return View(displayModel);
        }
    }
}