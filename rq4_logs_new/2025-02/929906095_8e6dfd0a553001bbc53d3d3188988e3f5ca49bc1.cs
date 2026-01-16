using BusinessObject.DTO;
using BusinessObject.Entities;
using BusinessObject.Interfaces;
using BusinessObject.Shares;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class HomeStayController(IRepository<HomeStay> _homeStayRepository,
        IRepository<User> _userRepository,
        IRepository<HomeStayImage> _homeStayImageRepository,
        IRepository<Calendar> _calendarRepository,
        IWebHostEnvironment _eviroment,
        IRepository<Amenity> _amenityRepository,
        IRepository<HomestayAmenity> _homeStayAmenity
            ) : ControllerBase
    {
        [HttpPost("add-home-stay")]
        public async Task<IActionResult> AddHomeStay([FromHeader(Name = "X-User-Id")] Guid userID,[FromBody]AddHomeStayRequest request)
        {
                var user = await _userRepository.GetByIdAsync(userID);
                Guid homeStayID = Guid.NewGuid();
                HomeStay createHomeStay = new HomeStay
                {
                    Id = homeStayID,
                    MainImage = request.MainImage,
                    Name = request.Name,
                    Description = request.Description,
                    Address = request.Address,
                    CheckInTime = request.CheckInTime,
                    CheckOutTime = request.CheckOutTime,
                    isBooked = request.isBlocked,
                    isDeleted = request.IsDeleted,
                    City = request.City,
                    OpenIn = request.OpenIn,
                    Standar = request.Standar,
                    User = user
                };
                await _homeStayRepository.AddAsync(createHomeStay);
                var calendarTask = _calendarRepository.AddAsync(new Calendar
                {
                    Date = request.Date,
                    Price = request.Price,
                    isDeleted = request.IsDeleted,
                    HomeStay = createHomeStay
                });

            foreach (var image in request.Images)
                {
                    Guid imageID = Guid.NewGuid();
                    HomeStayImage addImage = new HomeStayImage
                    {
                        Id = imageID,
                        Image = image,
                        HomeStay = createHomeStay
                    };

                    await _homeStayImageRepository.AddAsync(addImage);
                }
                    await _homeStayImageRepository.SaveAsync();
                

            return Ok(new { message = "Add Home Stay Success." });
            }

        

        [HttpPut("edit-home-stay-information")]
        public async Task<IActionResult> EditHomeStay([FromBody]EditHomeStayInforRequest request)
        {
            try
            {
                var getHomeStay = await _homeStayRepository.GetByIdAsync(request.HomeStayID);
                if (getHomeStay == null)
                {
                    return NotFound();
                }
                getHomeStay.Name = request.Name ?? getHomeStay.Name;
                getHomeStay.MainImage = request.MainImage ?? getHomeStay.MainImage;
                getHomeStay.Standar = getHomeStay.Standar = request.Standar != 0 ? request.Standar : getHomeStay.Standar;
                getHomeStay.CheckOutTime = request.CheckOutTime ?? getHomeStay.CheckOutTime;
                getHomeStay.CheckInTime = request.CheckInTime ?? getHomeStay.CheckInTime;
                getHomeStay.Address = request.Address ?? getHomeStay.Address;
                getHomeStay.OpenIn = request.OpenIn = request.OpenIn != 0 ? request.OpenIn : getHomeStay.OpenIn;
                getHomeStay.Description = request.Description ?? getHomeStay.Description    ;
                await _homeStayRepository.UpdateAsync(getHomeStay);
                await _homeStayRepository.SaveAsync();
                return Ok(new { Message = "Update Home Stay Success" });
            }
            catch (Exception ex) {
                return StatusCode(500, new { Message = "An error occurred", Error = ex.Message });
            }
        }


       
        [HttpPost("add-home-stay-image")]
        public async Task<IActionResult> AddHomeStayImage([FromBody]HomeStayImageDTO request)
        {
            var getHomeStay = await _homeStayRepository.GetByIdAsync(request.HomeStayID);
            foreach (var item in request.Images) {

                Guid imageID = Guid.NewGuid();
                HomeStayImage addImage = new HomeStayImage
                {
                    Id = imageID,
                    Image = item,
                    HomeStay = getHomeStay,
                    isDeleted = false
                };
                await _homeStayImageRepository.AddAsync(addImage);
            }
            await _homeStayImageRepository.SaveAsync();
            return Ok(new {Message = "Add Image Success"});
        }

        

    }
}
