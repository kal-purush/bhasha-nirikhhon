using Infrastructure.EF.DbEntities;
using Infrastructure.EF.HistoricArticle;
using Microsoft.AspNetCore.Mvc;

namespace WebApiTroc.Controllers;

[ApiController]
[Route("api/v1/HistoricArticle")]
public class HistoricArticleController:ControllerBase
{
    private readonly IHistoricArticle _historicArticle;

    public HistoricArticleController(IHistoricArticle historicArticle)
    {
        _historicArticle = historicArticle;
    }
    [HttpGet]
    [Route("getAll")]
    public IEnumerable<DbHistoricArticle> GetAll()
    {
        return _historicArticle.GetAll();
    }
    
    [HttpGet]    
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public DbHistoricArticle ArchiveTransaction(int idArticle)
    {
        return _historicArticle.archiveArticle(idArticle);
    }
    [HttpGet]
    [Route("Id/{id:int}")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult<DbHistoricArticle> FetchById(int id)
    {
        try
        {
            return _historicArticle.FetchById(id);
        }
        catch (KeyNotFoundException e)
        {
            return NotFound(new
            {
                e.Message
            });
        }
    }
}