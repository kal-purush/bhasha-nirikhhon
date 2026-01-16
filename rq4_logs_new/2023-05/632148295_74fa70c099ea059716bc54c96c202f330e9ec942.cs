using Microsoft.AspNetCore.Mvc;
using Pollaris.Managers;
using System.Dynamic;
using System.Text.Json;

namespace Pollaris.Controllers
{
    public class OptionController : Controller
    {
        [HttpPost]
        [Route("/Option/MakeOptionCorrect")]
        public void MakeOptionCorrect(int optionId)
        {

        }

        [HttpPost]
        [Route("/Option/CreateNewOption")]
        public int CreateNewOption(int questionId)
        {
            //Return the option's id
            return 0; 
        }

        [HttpPost]
        [Route("/Option/DeleteOption")]
        public void DeleteOption(int questionId)
        {
            //delete from questionOptions
            //delete from response
            //delete from Option
        }

        [HttpPost]
        [Route("/Option/ChangeOptionName")]
        public void ChangeOptionName(int optionId, string optionName)
        {
            
        }
    }
}