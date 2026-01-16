using API.Dtos.ImageDto;
using Domain.Entities;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Services.SqlDatabaseContextService;

namespace API.Controllers
{
    [Route("api/hotels/{hotelId}/rooms/{roomId}/images")]
    [ApiController]
    public class HotelRoomImageGalleryController : ControllerBase
    {
        private readonly IRoomManagementContextService roomRepository;
        private readonly ILogger<HotelRoomImageGalleryController> logger;
        public HotelRoomImageGalleryController(IRoomManagementContextService roomRepository, ILogger<HotelRoomImageGalleryController> logger)
        {
            this.roomRepository = roomRepository;
            this.logger = logger;
        }
        [HttpGet(Name = "GetAllRoomImages")]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(ICollection<HotelRoomImageGallery>))]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<IActionResult> GetAllAsync(int roomId)
        {
            try
            {
                var images = await this.roomRepository.GetAllRoomImagesByIdAsync(roomId);
                if (images == null)
                    return NotFound();
                return Ok(images);
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Unhandled exception from HotelRoomImageGalleryController.GetAllAsync");
                return Problem("Unable to get all the images from the room id provided");
            }
        }
        [HttpPost(Name = "AddImagesToRoom")]
        [ProducesResponseType(StatusCodes.Status201Created, Type = typeof(ICollection<CreateResult>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(CreateResult))]
        public async Task<IActionResult> AddImagesAsync(int roomId, [FromBody] ImageDto roomImageDto)
        {
            try
            {
                // Must have at least 1 and less than 4 images.
                if (roomImageDto.ImageUrls.Count() > 4 || !roomImageDto.ImageUrls.Any())
                {
                    return BadRequest(new CreateResult
                    {
                        Success = false,
                        ErrorMessages = ModelState.ServerError("Must have at least 1 and less than 4 images.")
                    });
                }
                var results = await this.roomRepository.AddRoomImagesByIdAsync(roomId, roomImageDto.ImageUrls);
                return CreatedAtRoute("AddImagesToRoom", new { roomId }, results);
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Unhandled exception from HotelRoomImageGalleryController.AddImagesAsync");
                return Problem("Unable to add images to the room id provided");
            }
        }
        [HttpDelete(Name = "DeleteRoomImages")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(DeleteResult))]
        public async Task<IActionResult> DeleteImagesAsync(int roomId, [FromBody] ImageDtoToBeDeleted imageDto)
        {
            try
            {
                // check if these ids are valid
                var invalidIds = new List<int>();
                foreach (var imageId in imageDto.imageIds)
                {
                    var image = await this.roomRepository.GetRoomImageByIdAsync(roomId, imageId);
                    if (image == null)
                    {
                        invalidIds.Add(imageId);
                    }
                }

                if (invalidIds.Any())
                {
                    return BadRequest(new DeleteResult
                    {
                        Success = false,
                        ErrorMessages = ModelState.ServerError("Cannot delete some images because some of them are invalid!")
                    });
                }
                await this.roomRepository.DeleteRoomImagesByIdAsync(imageDto.imageIds, roomId);
                return Ok();
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Unhandled exception from HotelRoomImageGalleryController.DeleteImagesAsync");
                return Problem("Unable to delete these images");
            }
        }
    }
}