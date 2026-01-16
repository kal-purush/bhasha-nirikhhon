using Messenger.core.Data;
using Messenger.core.Service;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace MessengerAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AboutUsController : ControllerBase
    {
        private readonly IAboutUsService aboutUs;
        public AboutUsController(IAboutUsService aboutUs)
        {
            this.aboutUs = aboutUs;
        }
        [HttpGet]
        [Route("GetAbout")]
        public IActionResult GetAbout()
        {
            var result = aboutUs.GetAbout();
            return Ok(result);
        }
        [HttpPut]
        [Route("UpdateAbout")]
        public IActionResult UpdateAbout(aboutUs about)
        {
            var result = aboutUs.UpdateAbout(about);
            return Ok(result);
        }
        [HttpPost]
        [Route("upLoadImg")]
        public aboutUs UploadImage()
        {
            try
            {
                var file = Request.Form.Files[0];
                var fileName = Guid.NewGuid().ToString() + "-" + file.FileName;
                var fullPath = Path.Combine("C:\\Users\\yazan\\OneDrive\\سطح المكتب\\New folder (2)\\MessengerAppUI\\src\\assets\\Img", fileName);

                using (var stream = new FileStream(fullPath, FileMode.Create))
                {
                    file.CopyTo(stream);
                }

                aboutUs item = new aboutUs();
                item.imgPaht= fileName;
                return item;
            }
            catch (Exception e)
            {
                return null;

            }

        }
    }
}