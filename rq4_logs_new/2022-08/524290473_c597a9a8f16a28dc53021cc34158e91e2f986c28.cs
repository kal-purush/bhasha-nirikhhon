using Microsoft.AspNetCore.Mvc;

namespace HelloGit.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class VehicleController : ControllerBase
    {

        [HttpGet("list")]
        public IEnumerable<Vehicle> List()
        {
            List<Vehicle> vehicleList = new List<Vehicle>();
            vehicleList.Add(
                new Vehicle
                {
                    Maker = "Subaru",
                    Model = "Impressa",
                    Price = 15000
                });
            vehicleList.Add(
           new Vehicle
           {
               Maker = "Mazda",
               Model = "3",
               Price = 12000
           });
            return vehicleList;
        }

        [HttpGet()]
        public Vehicle Get()
        {
            return new Vehicle
            {
                Maker = "Subaru",
                Model = "Impressa",
                Price = 15000
            };
        }
    }
}