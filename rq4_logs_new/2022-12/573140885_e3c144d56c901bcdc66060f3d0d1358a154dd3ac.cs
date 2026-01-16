using Microsoft.AspNetCore.Mvc;

using Model;

using Utils;

namespace App.Controllers;

[ApiController]
[Route("[controller]")]
public class Controller : ControllerBase
{
    [HttpGet]
    public ActionResult Get()
    {
        try
        {

            string filepath = Path.Join(".", "data.csv");
            List<Customer> customers = CSVHandler.Read(filepath);
            foreach (var customer in customers)
            {
                Console.WriteLine(customer.FullName);
            }

            return Ok();
        }
        catch
        {
            return BadRequest();
        }
    }
}