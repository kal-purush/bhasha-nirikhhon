using Domain;
using Infrastructure.EF.DbEntities;
using Infrastructure.EF.Transaction;
using Microsoft.AspNetCore.Mvc;

namespace WebApiTroc.Controllers;

[ApiController]
[Route("api/v1/Transaction")]

public class TransactionController : ControllerBase
{
    private readonly ITransaction _ITransaction;

    public TransactionController(ITransaction iTransaction)
    {
        _ITransaction = iTransaction;
    }

    [HttpGet]
    public IEnumerable<DbTransaction> GetAll()
    {
        return _ITransaction.GetAll();
    }
    
    [HttpPost]
    public ActionResult<Transaction> Create( DateTime Datest, int Id_user1, int Id_user2, string Article1, string Article2)
    {
        return Ok(_ITransaction.Create( Datest, Id_user1, Id_user2, Article1,Article2));
    }

    [HttpPut]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult Update(DbTransaction dbTransaction)
    {
        return _ITransaction.Update(dbTransaction) ? NoContent() : NotFound();
    }
    
    [HttpDelete]
    [Route("{id:int}")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult Delete(int id)
    {
        return _ITransaction.Delete(id) ? NoContent() : NotFound();
    }

}