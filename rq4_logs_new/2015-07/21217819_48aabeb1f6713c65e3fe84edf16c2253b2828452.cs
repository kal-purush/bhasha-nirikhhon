using System;
using System.Web.Mvc;
using Chess.Controllers;

namespace Chess.Filters
{
    public class VerifyIsParticipantAttribute : FilterAttribute, IAuthorizationFilter
    {
        public void OnAuthorization(AuthorizationContext filterContext)
        {
            int id;
            var idText = filterContext.RequestContext.HttpContext.Request.Params["id"];
            if (idText == null)
                return;

            if (!Int32.TryParse(idText, out id))
                return;

            var boardController = filterContext.Controller as BoardController;
            if (boardController == null)
                return;

            if (boardController.MayManipulateBoard(id))
                return;

            var jsonResponse = new JsonResult
            {
                Data = new {success = false, message = "You are not allowed to alter this board", status = "AUTH"}
            };

            filterContext.Result = jsonResponse;
        }
    }
}