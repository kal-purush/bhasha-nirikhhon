using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using EyesOnTheNet.DAL;
using System.IdentityModel.Tokens.Jwt;
using EyesOnTheNet.Models;

// For more information on enabling Web API for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace EyesOnTheNet.Controllers
{
    public class FileController : Controller
    {        // GET api/values/5
        [HttpGet("api/[controller]")]
        public IActionResult TestController()
        {
            return Ok("This Controller Works");
        }

        // GET api/values/5
        [HttpGet("api/[controller]/{cameraId:int}")]
        public async Task<IActionResult> SaveSingleCameraPicture(int cameraId)
        {
            string currentUser = new JwtSecurityToken(Request.Cookies["access_token"]).Subject;
            Camera returnedCamera = new EyesOnTheNetRepository().CanAccessThisCamera(currentUser, cameraId);
            string PictureReponse = await new FileRequests().SaveCameraPhoto(returnedCamera);

            return Ok(PictureReponse);
        }
    }
}