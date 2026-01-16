using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Http.Description;
using Web2ProjekatBackend.Models;

namespace Web2ProjekatBackend.Controllers
{
    public class PlanoviRadaController : ApiController
    {
        // GET: PlanoviRada
        Service.IWebService proxy;
        public PlanoviRadaController()
        {
            proxy = new Service.WebService();
        }

        [System.Web.Http.Authorize]
        [ResponseType(typeof(Models.PlanRada))]
        public IHttpActionResult Put(string id, [FromBody] PlanRada planRada)
        {
            if(planRada.IdPlana != id)
            {
                return BadRequest();
            }
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            try
            {
                proxy.updateEntity(planRada);
            }
            catch(Exception e) {return BadRequest();}
            return Ok(proxy.getEntity(TipEntiteta.PLANOVI, planRada.IdPlana));
        }
        [System.Web.Http.Authorize]
        [ResponseType(typeof(Models.PlanRada))]
        public IHttpActionResult Post(PlanRada planRada)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            proxy.addEntity(planRada);
            return CreatedAtRoute("DefaultApi", new { id = planRada.IdPlana }, planRada);
        }
        [Authorize]
        [ResponseType(typeof(void))]
        public IHttpActionResult Delete(string id)
        {
            ApplicationDbContext context = new ApplicationDbContext();
            PlanRada f = context.PlanoviRada.ToList().Find(x => x.IdPlana.Equals(id));
            if (f == null)
            {
                return NotFound();
            }

            proxy.deleteEntity(f);
            return Ok();
        }
        [Authorize]
        [ResponseType(typeof(PlanRada))]
        public IHttpActionResult Get(string id)
        {
            PlanRada f = proxy.getEntity(TipEntiteta.PLANOVI, id) as PlanRada;
            if (f == null)
            {
                return NotFound();
            }
            return Ok(f);
        }
        public IEnumerable<PlanRada> Get()
        {
            List<PlanRada> pr = new List<PlanRada>();
            foreach(object item in proxy.getEntities(TipEntiteta.PLANOVI))
            {
                pr.Add(item as PlanRada);
            }
            return pr;
        }

    }
}