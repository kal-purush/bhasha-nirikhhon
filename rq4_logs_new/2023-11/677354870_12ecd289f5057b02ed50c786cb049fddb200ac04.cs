using Microsoft.AspNetCore.Mvc;
using Mod4.Lection4.Hw.Context;

namespace Mod4.Lection4.Hw.Controllers;

[Route("api/[controller]")]
[ApiController]
public class EfTestController : ControllerBase  // депенденсі інжекшен
{
    private readonly EFCoreContext _dbContext;

    public EfTestController(EFCoreContext dbContext) => _dbContext = dbContext;

    [HttpGet]  // метод GET
    public async Task<ActionResult> TestConnection()
    {
        if (_dbContext.Database.CanConnect())  // перевірка, що ми законектились
        {
            return Ok(_dbContext.Database.ProviderName);  // респонс успішного конекту
        }

        return BadRequest(); // якщо конект не пройшов
    }
}