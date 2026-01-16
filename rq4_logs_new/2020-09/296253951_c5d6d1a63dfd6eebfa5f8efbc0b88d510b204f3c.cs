using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using Generator.Models;

namespace NamesGenerator.Controllers
{
    [ApiController]
    [Route("/[controller]")]
public class NamesController : Controller
{
    private readonly IDataGenerator<Name> _nameGeneratorService;

    public NamesController(IDataGenerator<Name> dataGeneratorService)
    {
        _nameGeneratorService = dataGeneratorService;
    }

    // GET: /names
    [HttpGet]
    public IEnumerable<Name> Get()
    {
        var data = _nameGeneratorService.Collection(20);
        return data;
    }
}
}