using BusinessLayer;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using ModelsLayer;


namespace ApiLayer.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ersControllers : ControllerBase
    {
        private readonly ersBusinessLayer _businessLayer;
        public ersControllers()//Constructor for ersControllers
        {
            this._businessLayer = new ersBusinessLayer();
        }



        [HttpPost("Login")]
        public async Task<ActionResult> LoginAsync(string Username, string Password)
        {

            bool SuccessfullLogin = await this._businessLayer.LoginAsync(Username, Password);
            return Ok(SuccessfullLogin);
        }



        [HttpPost("RegisterUser")]
        public async Task<ActionResult> RegisterUserAsync(Guid EmployeeID, string FirstName, string LastName, string Username, string Password)
        {
            bool SuccessfullyRegistered = await this._businessLayer.RegisterUserAsync(EmployeeID, FirstName, LastName, Username, Password);
            return Ok(SuccessfullyRegistered);
        }



        [HttpPost("SubmitTicket")]
        public async Task<ActionResult> SubmitTicketAsync(Guid TicketID, Guid fK_Employee, string Description, decimal Amount)
        {
            bool SuccessfullySubmited = await this._businessLayer.SubmitTicketAsync(TicketID, fK_Employee, Description, Amount);
            return Ok(SuccessfullySubmited);
        }



        [HttpPut("UpdateTicket")]
        public async Task<ActionResult<UpdatedTicketDTO>> JimmyAsync(ApprovalDTO approval) //JimmyAsync() is just a name for the method, it can be whatever you want. 
        {
            //Model binding matches body of request to the type of model we say we need.
            //ModelState is built in
            if (ModelState.IsValid)
            {
                //send ApprovalDTO to business layer
                UpdatedTicketDTO approvedRequest = await this._businessLayer.UpdateTicketAsync(approval);
                return Ok(approvedRequest);
            }
            else return Conflict(approval); //shows status code
        }



        [HttpGet("GetTickets")]
        public async Task<List<Ticket>> GetAllTicketsAsync(int Status)
        {
            List<Ticket> list = await this._businessLayer.GetAllTicketsAsync(Status);
            return list;
        }


        //Used to ensure username was not already registered
        [HttpGet("DoesUsernameAlreadyExist")]
        public async Task<ActionResult> DoesUsernameAlreadyExistAsync(string Username)
        {
            bool DoesExist = await this._businessLayer.DoesUsernameAlreadyExistAsync(Username);
            return Ok(DoesExist);
        }


        //Used so tickets cannot change status after processing
        [HttpGet("IsItAlreadyProcessed")]
        public async Task<ActionResult> IsItAlreadyProcessedAsync(Guid TicketID)
        {
            bool IsItProcessed = await this._businessLayer.IsitAlreadyProcessedAsync(TicketID);
            return Ok(IsItProcessed);
        }
    }
}



