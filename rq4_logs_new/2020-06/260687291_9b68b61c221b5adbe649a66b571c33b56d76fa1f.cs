using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using AutoMapper;
using CloudinaryDotNet;
using CloudinaryDotNet.Actions;
using DatingApp.API.Data;
using DatingApp.API.Dtos;
using DatingApp.API.Helpers;
using DatingApp.API.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace DatingApp.API.Controllers
{
    [Authorize]
    [Route("api/users/{userId}/photos")]
    [ApiController]
    public class PhotosController : ControllerBase
    {
        private readonly IDatingRepository _repo;
        private readonly IMapper _mapper;
        private readonly IOptions<CloudinarySettings> _options;
        private Cloudinary _cloudinary;

        public PhotosController(IDatingRepository repo, IMapper mapper, IOptions<CloudinarySettings> options)
        {
            _options = options;
            _mapper = mapper;
            _repo = repo;

            Account acc = new Account(
                options.Value.CloudName,
                options.Value.ApiKey,
                options.Value.ApiSecret
            );

            _cloudinary = new Cloudinary(acc);

        }

        [HttpGet("{id}" , Name="GetPhoto")]
        public async Task<IActionResult> GetPhoto(int id )
        {

            var photosFromRepo = await _repo.GetPhoto(id);

            var photo = _mapper.Map<PhotoForReturnDto>(photosFromRepo);

            return Ok(photo);
        }

        [HttpPost]
        public async Task<IActionResult> AddPhotoForUser(int uerId , PhotoForCreationDto photoForCreationDto)
        {
            if(uerId != int.Parse(User.FindFirst(ClaimTypes.NameIdentifier).Value))
                return Unauthorized();// 401 status code

                var userFromRepo = await _repo.GetUser(uerId);


                var file = photoForCreationDto.File;
                var uploadResult = new ImageUploadResult();

                if(file.Length > 0)
                {
                    using(var stream = file.OpenReadStream()) {

                        var uploadParams = new ImageUploadParams()
                        {
                           File = new FileDescription(file.Name , stream),
                           Transformation = new Transformation().Width(500).Height(500).Crop("fill").Gravity("face")     

                        };

                        uploadResult = _cloudinary.Upload(uploadParams);
                    }   

                }

                photoForCreationDto.Url = uploadResult.Uri.ToString();
                photoForCreationDto.PublicId = uploadResult.PublicId;

                var photo = _mapper.Map<Photo>(photoForCreationDto);

                if(!userFromRepo.Photos.Any(u => u.IsMain))
                    photo.IsMain = true;

                    userFromRepo.Photos.Add(photo);

                    if(await _repo.SaveAll())
                    {
                        var photoToReturn = _mapper.Map<PhotoForReturnDto>(photo);
                        return CreatedAtRoute("GetPhoto",new {uerId = uerId , id = photo.Id  },photoToReturn); // 200
                    }

                    return BadRequest("Cud not update photo");// 400
        }



    }
}