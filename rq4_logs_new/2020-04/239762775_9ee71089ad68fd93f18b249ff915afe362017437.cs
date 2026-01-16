using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Database.Models;
using Lib;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace WebAPI.Controllers
{
    [ApiController]
    public class UserAssignmentController : APIController
    {
        public UserAssignmentController(Context context, ILogger<UserController> logger) : base(context, logger)
        {
        }

        [HttpGet]
        [Route("[controller]/{UserAssignmentId}")]
        public APIResponse<UserAssignment> GetUserAssignment(int UserAssignmentId)
        {
            APIResponse<UserAssignment> response = new APIResponse<UserAssignment>();
            response.Data = _context.UserAssignments.SingleOrDefault(ua => ua.Id == UserAssignmentId);

            response.Success = true;
            response.SuccessMessage = $"Hämtade elevs inlämning med id {UserAssignmentId}";
            return response;
        }
        [HttpGet]
        [Route("[controller]/GetAllByUser/{UserId}")]
        public APIResponse<IEnumerable<UserAssignment>> GetAllByUser(int UserId)
        {
            APIResponse<IEnumerable<UserAssignment>> response = new APIResponse<IEnumerable<UserAssignment>>();
            response.Data = _context.UserAssignments.Where(ua => ua.UserId == UserId);

            response.Success = true;
            response.SuccessMessage = $"Hämtade alla inlämningar till elev id: {UserId}";
            return response;
        }
        [HttpGet]
        [Route("[controller]/GetAll")]
        public APIResponse<IEnumerable<UserAssignment>> GetAll()
        {
            APIResponse<IEnumerable<UserAssignment>> response = new APIResponse<IEnumerable<UserAssignment>>();
            response.Data = _context.UserAssignments.Select(ua => ua);

            response.Success = true;
            response.SuccessMessage = $"Hämtade alla student inlämningar";
            return response;
        }
        [HttpPost]
        [Route("[controller]/Add")]
        public APIResponse<UserAssignment> Add(UserAssignment userAssignment)
        {
            APIResponse<UserAssignment> response = new APIResponse<UserAssignment>();
            if(userAssignment != null)
            {
                _context.UserAssignments.Add(userAssignment);
                _context.SaveChanges();
                response.Data = userAssignment;
                response.Success = true;
                response.SuccessMessage = $"Du har lämnat in en inlämning";
            }
            else
            {
                response.FailureMessage = $"Det gick inte lämna in inlämning";
                response.Success = false;
            }
            return response;
        }
    }
}