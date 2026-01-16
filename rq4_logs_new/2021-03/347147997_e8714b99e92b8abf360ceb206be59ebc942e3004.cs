using DoodleWebMvc.Models;
using MarisDoodleLibrary.Contracts.Routines;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

namespace DoodleWebMvc.Controllers
{
    public class VoteController : Controller
    {
        private readonly IPollRoutine _pollRoutine;

        public VoteController(IPollRoutine pollRoutine)
        {
            _pollRoutine = pollRoutine;
        }

        public async Task<IActionResult> Index(int id)
        {
            PollFullModel displayModel = new PollFullModel();

            displayModel.Poll = await _pollRoutine.GetBasicPoll(id);
            displayModel.Options = await _pollRoutine.GetPollOptions(id);

            return View(displayModel);
        }
    }
}