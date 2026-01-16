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
    public class EkipaController : ApiController
    {
        // GET: Ekipa
        Service.IWebService proxy;
        public EkipaController()
        {
            proxy = new Service.WebService();
        }

        [System.Web.Http.Authorize]
        [ResponseType(typeof(Models.Ekipa))]
        public IHttpActionResult Put(string id, [FromBody] Ekipa ekipa)
        {
            if (ekipa.IdEkipe != id)
            {
                return BadRequest();
            }
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            try
            {
                proxy.updateEntity(ekipa);
            }
            catch (Exception e) { return BadRequest(); }
            return Ok(proxy.getEntity(TipEntiteta.EKIPE, ekipa.IdEkipe));
        }

        [System.Web.Http.Authorize]
        [ResponseType(typeof(Models.Ekipa))]
        public IHttpActionResult Post(Ekipa ekipa)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            proxy.addEntity(ekipa);
            return CreatedAtRoute("DefaultApi", new { id = ekipa.IdEkipe }, ekipa);
        }
        [System.Web.Http.Authorize]
        [ResponseType(typeof(void))]
        public IHttpActionResult Delete(string id)
        {
            ApplicationDbContext context = new ApplicationDbContext();
            Ekipa ek = context.Ekipe.ToList().Find(x => x.IdEkipe.Equals(id));
            if (ek == null)
            {
                return NotFound();
            }

            proxy.deleteEntity(ek);
            return Ok();
        }
        [System.Web.Http.Authorize]
        [ResponseType(typeof(Ekipa))]
        public IHttpActionResult Get(string id)
        {
            Ekipa ek = proxy.getEntity(TipEntiteta.EKIPE, id) as Ekipa;
            if (ek == null)
            {
                return NotFound();
            }
            return Ok(ek);
        }
        public IEnumerable<Ekipa> Get()
        {
            List<Ekipa> ek = new List<Ekipa>();
            foreach (object item in proxy.getEntities(TipEntiteta.EKIPE))
            {
                ek.Add(item as Ekipa);
            }
            return ek;
        }

    }
}