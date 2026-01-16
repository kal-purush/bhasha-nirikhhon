using Backend.DataAccessObjects.Comment;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using Shared;

namespace Backend.Controllers;


[ApiController]
[Route("[controller]")]
public class CommentController : ControllerBase
{
    private ICommentInterface _commentInterface;

    public CommentController(ICommentInterface commentInterface)
    {
        _commentInterface = commentInterface;
    }

    [EnableCors]
    [HttpPost]
    public async Task<ActionResult> AddCommentToMovie(int userid, long movieid, string comment)
    {
        try
        {
            await _commentInterface.AddCommentToMovie(userid,movieid,comment);
            return StatusCode(200);
        }
        catch (Exception e)
        {
            return   StatusCode(500, e.Message);
        }
    }
}