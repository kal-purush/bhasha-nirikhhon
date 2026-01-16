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
    public class OpremaController : ApiController
    {
        // GET: Oprema
        Service.IWebService proxy;
        public OpremaController()
        {
            proxy = new Service.WebService();
        }

        //[System.Web.Http.Authorize]
        [ResponseType(typeof(Models.Oprema))]
        public IHttpActionResult Put(string id, [FromBody] Oprema ekipa)
        {
            if (ekipa.IdOprema != id)
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
            return Ok(proxy.getEntity(TipEntiteta.OPREMA, ekipa.IdOprema));
        }

        //[System.Web.Http.Authorize]
        [ResponseType(typeof(Models.Oprema))]
        public IHttpActionResult Post(Oprema ekipa)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            proxy.addEntity(ekipa);
            return CreatedAtRoute("DefaultApi", new { id = ekipa.IdOprema }, ekipa);
        }
        //[System.Web.Http.Authorize]
        [ResponseType(typeof(void))]
        public IHttpActionResult Delete(string id)
        {
            ApplicationDbContext context = new ApplicationDbContext();
            Oprema ek = context.Oprema.ToList().Find(x => x.IdOprema.Equals(id));
            if (ek == null)
            {
                return NotFound();
            }

            proxy.deleteEntity(ek);
            return Ok();
        }
        //[System.Web.Http.Authorize]
        [ResponseType(typeof(Oprema))]
        public IHttpActionResult Get(string id)
        {
            Oprema ek = proxy.getEntity(TipEntiteta.OPREMA, id) as Oprema;
            if (ek == null)
            {
                return NotFound();
            }
            return Ok(ek);
        }
        public IEnumerable<Oprema> Get()
        {
            List<Oprema> ek = new List<Oprema>();
            foreach (object item in proxy.getEntities(TipEntiteta.OPREMA))
            {
                ek.Add(item as Oprema);
            }
            return ek;
        }
    }
}