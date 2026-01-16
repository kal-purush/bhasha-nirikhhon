using Microsoft.AspNetCore.Mvc;
using Igtampe.Neco.Common;
using Igtampe.Neco.Data;
using Igtampe.ChopoSessionManager;
using Microsoft.EntityFrameworkCore;

namespace Igtampe.Neco.API.Controllers {

    /// <summary>Controller that handles User operations</summary>
    [Route("API/Images")]
    [ApiController]
    public class ImageController : ControllerBase {

        private readonly NecoContext DB;

        /// <summary>Creates a User Controller</summary>
        /// <param name="Context"></param>
        public ImageController(NecoContext Context) => DB = Context;

        /// <summary>Gets an image from the DB</summary>
        /// <param name="ID">ID of the image to retrieve</param>
        /// <returns></returns>
        [HttpGet("{ID}")]
        public async Task<IActionResult> GetImage(Guid ID) {

            Image? I = await DB.Image.FindAsync(ID);
            return I is null || I.Data is null || I.Type is null ? NotFound("Image was not found") : File(I.Data, I.Type);
        }

        /// <summary>Uploads an Image to the DB.</summary>
        /// <param name="SessionID">ID of the session executing this request</param>
        /// <returns></returns>
        // POST api/Images
        [HttpPost]
        public async Task<IActionResult> UploadImage([FromHeader] Guid SessionID) {

            //Check the session:
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID));
            if (S is null) { return Unauthorized("Invalid Session"); }

            //Find the user
            User? U = await DB.User.Include(O => O.Roles).FirstOrDefaultAsync(O=> O.ID == S.UserID);
            if(U is null) { return Unauthorized("Invalid Session"); }

            if (!U.Roles.ImageUploader) { return Forbid(); }

            //That is all we will check the session for. In some future other project we may have an `Image Uploader` role to verify but for now this is fine
            string? ContentType = Request.ContentType;

            int MaxSize = 1024 * 1024 * 1;

            if (ContentType != "image/png" && ContentType != "image/jpeg" && ContentType != "image/gif") { return BadRequest("File must be PNG, JPG, or GIF"); }
            if (Request.ContentLength > MaxSize) { return BadRequest("File must be less than 1mb in size"); }

            Image I = new() { Type = ContentType };

            using (var memoryStream = new MemoryStream()) {
                await Request.Body.CopyToAsync(memoryStream);
                I.Data = memoryStream.ToArray();
                if (I.Data.Length > MaxSize) { return BadRequest("File must be less than 1mb in size"); }
            }

            DB.Image.Add(I);
            await DB.SaveChangesAsync();

            return Created($"/API/Images/{I.ID}.png", I.ID);

        }
    }
}