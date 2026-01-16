using System.Web.Mvc;
using ItAcademy.PropertyCenter.Entities;
using ItAcademy.PropertyCenter.Services;
using Microsoft.Practices.Unity;

namespace ItAcademy.PropertyCenter.Controllers
{
    public class AnnouncementController : Controller
    {
        [Dependency]
        public IAnnouncementService AnnouncementService { private get; set; }
        
        public ActionResult Index()
        {
            var announcements = AnnouncementService.GetAnnouncements();

            return View(announcements);
        }

        [HttpGet]
        public ActionResult Create()
        {
            var announcement = new Announcement();

            return View(announcement);
        }

        [HttpPost]
        public ActionResult Create(Announcement announcement)
        {
            if (ModelState.IsValid)
            {
                AnnouncementService.AddAnnouncement(announcement);
            }

            return View(announcement);
        }
    }
}