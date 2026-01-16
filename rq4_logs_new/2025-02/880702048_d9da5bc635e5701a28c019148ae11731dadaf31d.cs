using Microsoft.AspNetCore.Mvc;
using P2Project.Core.Dtos.Volunteers;
using P2Project.Framework;

namespace P2Project.Volunteers.Web;

public class TestController : ApplicationController
{
    [HttpGet]
    public ActionResult Get()
    {
        List<FullNameDto> fullNames = [
            new ("Vorontsov", "Oleg", "Ruslanovich"),
            new ("Vorontsova", "Ekaterina", "Aleksandrovna"),
            new ("Vorontsova", "Kapitolina", "Olegovna"),
        ];

        return Ok(fullNames);
    }
}