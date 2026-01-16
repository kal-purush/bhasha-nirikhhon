using BroadwayBuilder.Api.Models;
using ServiceLayer.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Http.ModelBinding;



namespace BroadwayBuilder.Api.Controllers
{
    public static class ControllerHelper
    {
         public static void ValidateLoginRequestModel(ModelStateDictionary modelState, object payload)
        {
            if (!modelState.IsValid || payload == null)
            {
                throw new InvalidLoginRequestModelException("Invalid payload.");
            }
        }

        public static Guid ParseAndCheckId(string id)
        {
            Guid guid;

            // check if valid SSO ID format
            var validParse = Guid.TryParse(id, out guid);
            if (!validParse)
            {
                throw new InvalidGuidException("Invalid Id.");
            }
            return guid;
        }
    }


}