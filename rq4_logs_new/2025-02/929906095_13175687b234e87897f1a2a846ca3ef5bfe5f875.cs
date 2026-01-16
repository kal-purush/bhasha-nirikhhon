using BusinessObject.DTO;
using BusinessObject.Entities;
using BusinessObject.Interfaces;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace RUNAHMS_API.Controllers
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
        public async Task<IActionResult> AddHomeStay([FromHeader(Name = "X-User-Id")] Guid userID, [FromBody] AddHomeStayRequest request)
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

        [HttpDelete("delete-home-stay")]
        public async Task<IActionResult> DeleteHomeStay([FromQuery]Guid homeStayID)
        {
            var checkDelete = await _homeStayRepository.GetByIdAsync(homeStayID);
            if (checkDelete != null && checkDelete.isDeleted == false)
            {
                checkDelete.isDeleted = true;
               await _homeStayRepository.UpdateAsync(checkDelete);
               await _homeStayRepository.SaveAsync();
                return Ok(new { Message = "Delete Success" }); 
            }
            else if (checkDelete != null && checkDelete.isDeleted == true)
            {
                checkDelete.isDeleted = false;
                await _homeStayRepository.UpdateAsync(checkDelete);
                await _homeStayRepository.SaveAsync();
                return Ok(new { Message = "Restore Home Stay Success" });
            }
            return NotFound();
        }

        [HttpPost("get-all-home-stay")]
        public async Task<IActionResult> GetAllHomeStay([FromBody]FilterDTO request)
        {
            var query = _homeStayRepository
                .FindWithInclude(h => h.Calendars!)
                .Include(h => h.HomestayAmenities!)
                .ThenInclude(ha => ha.Amenity)
                .Include(hf =>  hf.HomestayFacilities)
                .ThenInclude(fa => fa.Facility)
                .AsQueryable();

            if (request.Standard is { Count: > 0 })
            {
                query = query.Where(h => request.Standard.Contains(h.Standar));
            }

            if (request.AmenityNames is { Count: > 0 })
            {
                query = query.Where(h =>
                    h.HomestayAmenities!.Any(ha => request.AmenityNames.Contains(ha.Amenity.Name)));
            }


            if (request.MinPrice.HasValue || request.MaxPrice.HasValue)
            {
                query = query.Where(h => h.Calendars!.Any(c =>
                    (!request.MinPrice.HasValue || c.Price >= request.MinPrice.Value) &&
                    (!request.MaxPrice.HasValue || c.Price <= request.MaxPrice.Value)
                ));
            }

            var listHomeStay = await query.ToListAsync();

            if (!listHomeStay.Any())
            {
                return NotFound();
            }

            var response = listHomeStay.Select(h => new
            {
                h.Id,
                h.Name,
                h.MainImage,
                h.Address,
                h.City,
                h.CheckInTime,
                h.CheckOutTime,
                h.OpenIn,
                h.Description,
                h.Standar,
                h.isDeleted,
                h.isBooked,

                Calendar = h.Calendars!.Select(c => new
                {
                    c.Id,
                    c.Date,
                    c.Price
                }).ToList(),

                Amenities = h.HomestayAmenities!
                    .Select(ha => new
                    {
                        ha.Amenity.Id,
                        ha.Amenity.Name
                    }).ToList(),
                Facility = h.HomestayFacilities!.Select(hf => new
                {
                        hf.FacilityID,
                        hf.Facility.Name,
                        hf.Facility.Description
                }).ToList()
            }).ToList();

            return Ok(response);
        }

        [HttpGet("get-home-stay-detail")]
        public async Task<IActionResult> GetHomeStayDetail([FromQuery] Guid homeStayID)
        {
            var getDetail = await _calendarRepository
                .FindWithInclude(h => h.HomeStay)
                .Include(h => h.HomeStay!)
                .ThenInclude(hs => hs.HomestayAmenities!)
                .ThenInclude(ha => ha.Amenity)
                .Include(h => h.HomeStay.HomestayImages!)
                .FirstOrDefaultAsync(h => h.HomeStay.Id == homeStayID);

            if (getDetail == null)
            {
                return NotFound();
            }

            var response = new
            {
                getDetail.Id,
                getDetail.HomeStay.Name,
                getDetail.HomeStay.MainImage,
                getDetail.HomeStay.Address,
                getDetail.HomeStay.City,
                getDetail.HomeStay.CheckInTime,
                getDetail.HomeStay.CheckOutTime,
                getDetail.HomeStay.OpenIn,
                getDetail.HomeStay.Description,
                getDetail.HomeStay.Standar,
                getDetail.HomeStay.isDeleted,
                getDetail.HomeStay.isBooked,
                Calendar = _calendarRepository
                    .FindWithInclude(c => c.HomeStay)
                    .Where(c => c.HomeStay.Id == homeStayID)
                    .Select(c => new
                    {
                        c.Id,
                        c.Date,
                        c.Price
                    }).ToList(),
                HomeStayImage = getDetail.HomeStay.HomestayImages!.Select(image => new
                {
                    Image = image.Image,
                }),
                Amenities = getDetail.HomeStay.HomestayAmenities!.Select(ha => new
                {
                    ha.Amenity.Id,
                    ha.Amenity.Name
                }).ToList()
            };

            return Ok(response);
        }

        [HttpGet("filter-home-stay-with-status")]
        public async Task<IActionResult> FilterHomeStayWithStatus([FromQuery] bool status)
        {
            var filter = await _homeStayRepository.Find(x => x.isDeleted == status).ToListAsync();
            if(filter.Any())
            {
                return Ok(filter);

            }
            return NotFound();
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
