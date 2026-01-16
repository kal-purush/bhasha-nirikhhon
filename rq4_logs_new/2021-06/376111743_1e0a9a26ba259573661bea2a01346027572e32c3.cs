using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Http;
using System.Web.Http.Description;
using System.Web.Mvc;
using Web2ProjekatBackend.Models;

namespace Web2ProjekatBackend.Controllers
{
    public class NaloziRadaController : ApiController
    {
        // GET: PlanoviRada
        Service.IWebService proxy;
        public NaloziRadaController()
        {
            proxy = new Service.WebService();
        }

        [System.Web.Http.Authorize]
        [ResponseType(typeof(Models.NalogRada))]
        public IHttpActionResult Put(string id, [FromBody] NalogRada nalogRada)
        {
            if (nalogRada.IdNaloga != id)
            {
                return BadRequest();
            }
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            try
            {
                proxy.updateEntity(nalogRada);
            }
            catch (Exception e) { return BadRequest(); }
            return Ok(proxy.getEntity(TipEntiteta.NALOZI, nalogRada.IdNaloga));
        }
        [System.Web.Http.Authorize]
        [ResponseType(typeof(Models.NalogRada))]
        public IHttpActionResult Post(NalogRada nalogRada)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            proxy.addEntity(nalogRada);
            return CreatedAtRoute("DefaultApi", new { id = nalogRada.IdNaloga }, nalogRada);
        }
        [System.Web.Http.Authorize]
        [ResponseType(typeof(void))]
        public IHttpActionResult Delete(string id)
        {
            ApplicationDbContext context = new ApplicationDbContext();
            NalogRada f = context.NaloziRada.ToList().Find(x => x.IdNaloga.Equals(id));
            if (f == null)
            {
                return NotFound();
            }

            proxy.deleteEntity(f);
            return Ok();
        }
        [System.Web.Http.Authorize]
        [ResponseType(typeof(NalogRada))]
        public IHttpActionResult Get(string id)
        {
            NalogRada f = proxy.getEntity(TipEntiteta.NALOZI, id) as NalogRada;
            if (f == null)
            {
                return NotFound();
            }
            return Ok(f);
        }
        public IEnumerable<NalogRada> Get()
        {
            List<NalogRada> pr = new List<NalogRada>();
            foreach (object item in proxy.getEntities(TipEntiteta.NALOZI))
            {
                pr.Add(item as NalogRada);
            }
            return pr;
        }
    }
}