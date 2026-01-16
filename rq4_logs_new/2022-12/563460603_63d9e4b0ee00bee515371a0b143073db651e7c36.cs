using Infrastructure.EF.DbEntities;
using Infrastructure.EF.HistoricTransaction;
using Microsoft.AspNetCore.Mvc;

namespace WebApiTroc.Controllers;

[ApiController]
[Route("api/v1/HistoricTransaction")]
public class HistoricTransactionController:ControllerBase
{
    private readonly IHistoricTransaction _historicTransaction;

    public HistoricTransactionController(IHistoricTransaction historicTransaction)
    {
        _historicTransaction = historicTransaction;
    }

    [HttpGet]
    [Route("getAll")]
    public IEnumerable<DbHisotricTransaction> GetAll()
    {
        return _historicTransaction.GetAll();
    }
    
    [HttpGet]    
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public DbHisotricTransaction ArchiveTransaction(bool echange, int idtransaction)
    {
        return _historicTransaction.ArchiveTransaction(echange, idtransaction);
    }
    [HttpGet]
    [Route("fetchByIdUser")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status500InternalServerError)]
    public IEnumerable<DbHisotricTransaction> fetchByIdUser()
    {
        var id = User.Claims.First(claim => claim.Type == "id").Value;
        
        var idUser = Convert.ToInt32(id);
        return _historicTransaction.fetchByIdUser(idUser);
    }
    [HttpGet]
    [Route("fetchByIdUserOffer")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status500InternalServerError)]
    public IEnumerable<DbHisotricTransaction> fetchByIdUserOffer()
    {
        var id = User.Claims.First(claim => claim.Type == "id").Value;
        
        var idUser = Convert.ToInt32(id);
        return _historicTransaction.fetchByIdUserOffer(idUser);
    }
    
    [HttpGet]
    [Route("{id:int}")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult<DbHisotricTransaction> FetchById(int id)
    {
        try
        {
            return _historicTransaction.fetchById(id);
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