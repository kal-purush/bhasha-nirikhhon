using System;
using BusinessObject.DTO;
using BusinessObject.Entities;
using BusinessObject.Interfaces;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class UploadController(IRepository<HomeStayImage> _homeStayImageRepository,
                                  IRepository<HomeStay> _homeStayRepository,
                                  IWebHostEnvironment _enviroment,
                                  IConfiguration _configuration) : ControllerBase
    {
        [HttpPost("upload-image")]
        public IActionResult UploadImage([FromForm] UploadDTO upload)
        {
            if (upload.File == null || _enviroment == null)
            {
                throw new ArgumentNullException("Invalid image or environment settings.");
            }

            string uploadsFolder = Path.Combine(_enviroment.ContentRootPath, "images");
            if (!Directory.Exists(uploadsFolder))
            {
                Directory.CreateDirectory(uploadsFolder);
            }
            string uniqueFileName = "";
            if (upload.File.FileName == null)
            {
                uniqueFileName = Guid.NewGuid().ToString() + "_" + Path.GetFileName(upload.File.Name);
            }
            uniqueFileName = Guid.NewGuid().ToString() + "_" + Path.GetFileName(upload.File.FileName);
            string filePath = Path.Combine(uploadsFolder, uniqueFileName);

            using (var fileStream = new FileStream(filePath, FileMode.Create))
            {
                upload.File.CopyTo(fileStream);
            }
            string url = _configuration["Base:Url"] + "/images/" + uniqueFileName;

            return Ok(new { url = url });
        }
    }
}