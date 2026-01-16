using System.Collections.Generic;
using System.Web.Http;

namespace OverwatchSynergy.Api
{
    [RoutePrefix("objectives")]
    public class ObjectivesController : ApiController
    {
        [HttpGet, Route("")]
        public IEnumerable<ObjectiveType> Get()
        {
            return Calculator.ObjectiveTypes;
        }
    }
}