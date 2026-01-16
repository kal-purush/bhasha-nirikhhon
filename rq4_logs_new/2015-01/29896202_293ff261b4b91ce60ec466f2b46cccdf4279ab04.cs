using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using System.Net;
using NLog;

namespace echo
{
    public class EchoController : ApiController
    {
        public EchoController()
        {
            _log = LogManager.GetLogger("Echo");
        }

        public IHttpActionResult Get()
        {
            return Ok();
        }

        [Route("echo/{status}")]
        public IHttpActionResult Get(int status)
        {
            return Process(status, Request.Content.ReadAsStringAsync().Result);
        }

        [Route("echo/{status}")]
        public IHttpActionResult Post([FromUri]int status)
        {
            return Process(status, Request.Content.ReadAsStringAsync().Result);
        }

        IHttpActionResult Process(int status, string body)
        {
            IHttpActionResult result;
            switch ((HttpStatusCode)status)
            {
                case HttpStatusCode.OK:
                    _log.Info("HTTP/1.1 200 OK\n{0}\n", body);
                    result = Ok(status);
                    break;
                case HttpStatusCode.InternalServerError:
                    _log.Error("HTTP/1.1 500 Internal Server Error\n{0}\n", body);
                    result = InternalServerError();
                    break;
                default:
                    _log.Warn("HTTP/1.1 404 Not Found\n{0}\n", body);
                    result = NotFound();
                    break;
            }

            return result;
        }

        readonly Logger _log;
    }
}