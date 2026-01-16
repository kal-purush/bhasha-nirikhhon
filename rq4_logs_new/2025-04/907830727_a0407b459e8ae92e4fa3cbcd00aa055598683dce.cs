using AutoMapper;
using EasyTravel.Domain.Entites;
using EasyTravel.Domain.Services;
using EasyTravel.Web.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using System.Globalization;

namespace EasyTravel.Web.Controllers
{
    [Authorize]
    public class ReviewController : Controller
    {
        private readonly UserManager<User> _userManager;
        private readonly ISessionService _sessionService;
        private readonly IMapper _mapper;
        private readonly IPhotographerService _photographerService;
        private readonly IAgencyService _agencyService;
        private readonly IGuideService _guideService;
        public ReviewController(ISessionService sessionService, IPhotographerService photographerService,
            UserManager<User> userManager,IMapper mapper, IAgencyService agencyService, IGuideService guideService)
        {
            _sessionService = sessionService;
            _photographerService = photographerService;
            _userManager = userManager;
            _mapper = mapper;
            _agencyService = agencyService;
            _guideService = guideService;
        }
        public IActionResult Index()
        {
            return View();
        }
        [HttpGet]
        public async Task<IActionResult> Photographer()
        {
            var pgId = _sessionService.GetString("PhotographerId");
            if (string.IsNullOrEmpty(pgId))
            {
                return Redirect("/Photographer/List");
            }
            _sessionService.SetString("LastVisitedPage", "/Review/Photographer");
            var user = await _userManager.GetUserAsync(User);
            var pgBooking = _mapper.Map<PhotographerBookingViewModel>(user);
            var photographer = _photographerService.Get(Guid.Parse(pgId));
            pgBooking.Photographer = photographer;
            var agency = _agencyService.Get(photographer.AgencyId);
            pgBooking.AgencyName = agency.Name;
            pgBooking.EventDate = DateTime.Parse(_sessionService.GetString("EventDate"), CultureInfo.InvariantCulture);
            pgBooking.StartTime = TimeSpan.Parse(_sessionService.GetString("StartTime"), CultureInfo.InvariantCulture);
            pgBooking.EndTime = TimeSpan.Parse(_sessionService.GetString("EndTime"), CultureInfo.InvariantCulture);
            pgBooking.TimeInHour = int.Parse(_sessionService.GetString("TimeInHour"), CultureInfo.InvariantCulture);
            pgBooking.EventType = _sessionService.GetString("EventType");
            pgBooking.EventLocation = _sessionService.GetString("EventLocation");
            pgBooking.TotalAmount = photographer.HourlyRate * pgBooking.TimeInHour;
            return View(pgBooking);
        }
        [HttpGet]
        public async Task<IActionResult> Guide()
        {
            var guideId = _sessionService.GetString("GuideId");
            if (string.IsNullOrEmpty(guideId))
            {
                return Redirect("/Guide/List");
            }
            _sessionService.SetString("LastVisitedPage", "/Review/Guide");
            var user = await _userManager.GetUserAsync(User);
            var guideBooking = _mapper.Map<GuideBookingViewModel>(user);
            var guide = _guideService.Get(Guid.Parse(guideId));
            guideBooking.Guide = guide;
            var agency = _agencyService.Get(guide.AgencyId);
            guideBooking.AgencyName = agency.Name;
            guideBooking.EventDate = DateTime.Parse(_sessionService.GetString("EventDate"),CultureInfo.InvariantCulture);
            guideBooking.StartTime = TimeSpan.Parse(_sessionService.GetString("StartTime"), CultureInfo.InvariantCulture);
            guideBooking.EndTime = TimeSpan.Parse(_sessionService.GetString("EndTime"), CultureInfo.InvariantCulture);
            guideBooking.TimeInHour = int.Parse(_sessionService.GetString("TimeInHour"));
            guideBooking.EventType = _sessionService.GetString("EventType");
            guideBooking.EventLocation = _sessionService.GetString("EventLocation");
            guideBooking.TotalAmount = guide.HourlyRate * guideBooking.TimeInHour;
            return View(guideBooking);
        }
    }
}