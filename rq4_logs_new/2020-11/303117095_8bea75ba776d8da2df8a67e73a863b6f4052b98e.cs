using Microsoft.AspNetCore.Mvc;
using LabTestAsp.Models;

namespace WseiLabs.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ListingController : Controller
    {
        [HttpPost]
        public ListingModel Post(ListingModel listingModel)
        {
            return listingModel;
        }
    }
}