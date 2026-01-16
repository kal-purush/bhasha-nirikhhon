using Infrastructure.EF.Category;
using Infrastructure.EF.DbEntities;
using Microsoft.AspNetCore.Mvc;

namespace WebApiTroc.Controllers;

[ApiController]
[Route("api/v1/Category")]
public class CategoryController : ControllerBase
{
    private readonly ICategory _category;

    public CategoryController(ICategory category)
    {
        _category = category;
    }
    
        
    [HttpGet]
    public IEnumerable<DbCategory> GetAll()
    {
        return _category.GetAll();
    }
    
    [HttpPost]
    public ActionResult<DbCategory> Create(string nomCat)
    {
        return Ok(_category.Create(nomCat));
    }
    [HttpGet]
    [Route("{name}")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]

    public ActionResult<DbCategory> FetchByName(string name)
    {
        try
        {
            return _category.FetchByName(name);
        }
        catch (KeyNotFoundException e)
        {
            return NotFound(new
            {
                e.Message
            });
        }
    }
    
    [HttpPut]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult Update(DbCategory dbCategory)
    {
        return _category.Update(dbCategory) ? NoContent() : NotFound();
    }
    
    [HttpDelete]
    [Route("delete/{name}")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult Delete(string name)
    {
        return _category.Delete(name) ? NoContent() : NotFound();
    }

}