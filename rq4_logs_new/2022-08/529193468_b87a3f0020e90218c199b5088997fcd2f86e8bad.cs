namespace CoreTest.Controllers
{
    using Microsoft.AspNetCore.Mvc;

    [ApiController]
    [Route("[controller]")]
    public class CalcController : ControllerBase
    {
        [HttpGet(Name = "Add/{a}/{b}")]
        public int Add(int a, int b) => a + b;
        
        
        
    }
}