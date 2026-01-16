using Event_Planinng_System_DAL.Models;
using Event_Planinng_System_DAL.Repos;
using Event_Planinng_System_DAL.Unit_Of_Work;
using Event_Planning_System.DTO;
using Event_Planning_System.IServices;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.IO;

namespace Event_Planning_System.Controllers
{
    [Route("api/[controller]")]
    [ApiController]

    public class ProfileController : ControllerBase
    {
        IProfileService profileService;
        public ProfileController(IProfileService profileService)
        {
            this.profileService = profileService;
        }

        // Get: api/Profile/{id}
        [HttpGet("{id:int}")]
        [ProducesResponseType<ProfileDTO>(200)]
        [ProducesResponseType(404)]
        public async Task<ActionResult> GetProfileById(int id) 
        {
           var profileDTO = await profileService.GetProfileById(id);
            if (profileDTO == null)
            {
                return NotFound();
            }
            else
            {
                return Ok(profileDTO);
            }
            
        }

        // Get: api/Profile/{name}
        [HttpGet("email/{email}")]
        [ProducesResponseType<ProfileDTO>(200)]
        [ProducesResponseType(404)]
        public async Task<ActionResult> GetProfileByEmail(string email)
        {
            var ProfileDTO = await profileService.GetProfileByEmail(email);
            if (ProfileDTO == null)
            {
                return NotFound();
            }
            else
            {
                return Ok(ProfileDTO);
            }
        }

        // PUT: api/Profile/{id}
        [HttpPut("{id:int}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult> EditProfile(int id, [FromBody] ProfileDTO profileDTO)
        {
            if (profileDTO == null)
            {
                return BadRequest("Profile data is required.");
            }

            var existingProfile = await profileService.GetProfileById(id);
            if (existingProfile == null)
            {
                return NotFound($"Profile with id {id} not found.");
            }

            var updatedProfile = await profileService.EditProfile(id, profileDTO);
            if (updatedProfile == null)
            {
                return BadRequest("Failed to update the profile.");
            }

            return NoContent();
        }

        // DELETE: api/Profile/{id}
        [HttpDelete("{id:int}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult> DeleteProfile(int id)
        {
            var deletedProfile = await profileService.DeleteProfile(id);
            if (deletedProfile == null)
            {
                return NotFound($"Profile with id {id} not found.");
            }

            return NoContent();
        }



    }
}