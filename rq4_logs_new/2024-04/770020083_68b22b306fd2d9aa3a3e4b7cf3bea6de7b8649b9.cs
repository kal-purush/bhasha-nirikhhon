using Domain.Entities;
using Microsoft.AspNetCore.Mvc;

namespace HexagonalArchitecture.Controllers;

[ApiController]
[Route("")]
[Route("/api")]
[Route("/api/teste")]
public class TestController : ControllerBase
{
    [HttpGet]
    public string Get()
    {
        return "Testando";
    }

    [HttpGet]
    [Route("/api/teste2")]
    public string Get2()
    {
        return "testando2";
    }

    [HttpGet]
    [Route("/api/hotel")]
    public List<Hotel> listHotels()
    {
        Hotel h1 = new Hotel("Hotel1", "address", 4234.32, 4324.32, null);
        Hotel h2 = new Hotel();

        return new List<Hotel>() { h1, h2 };
    }
}