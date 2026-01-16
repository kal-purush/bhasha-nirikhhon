using dbcontext;
using dbcontext.Classes;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Linktindr_be.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class OpdrachtgeverController : ControllerBase
    {
        OurContext OU;
        public OpdrachtgeverController(OurContext oU)
        {
            OU = oU;
        }

        // GET: api/<ValuesController>
        [HttpGet]
        public IEnumerable<Opdrachtgever> Get()
        {
            return OU.opdrachtgever;
        }

        // GET (specific) api/<OpdrachtgeverController>/{id}
        [HttpGet("{id}")]
        public Opdrachtgever Get(int id)
        {
            Opdrachtgever t = OU.opdrachtgever.Find(id);

            return t;
        }

        // ADD api/<OpdrachtgeverController>/add
        [HttpPost("add")]
        public string Add(Opdrachtgever_NoId oni)
        {
            Opdrachtgever o = new Opdrachtgever();
            o.Name = oni.Name;
            o.Email = oni.Email;
            o.Telephone = oni.Telephone;

            OU.Add(o);
            OU.SaveChanges();
            return "gelukt";
        }

        // PUT api/<OpdrachtgeverController>/update
        [HttpPut("update")]
        public string Put(Opdrachtgever o)
        {
            Opdrachtgever oou = OU.opdrachtgever.Find(o.Id);
            if (oou == null)
            {
                return "gefaald";
            }

            oou.Name = o.Name;
            oou.Email = o.Email;
            oou.Telephone = o.Telephone;

            OU.opdrachtgever.Update(oou);
            OU.SaveChanges();

            return "gelukt";
        }

        // DELETE api/<OpdrachtgeverController>/delete
        [HttpDelete("delete")]
        public string Delete(int id)
        {
            Opdrachtgever t = OU.opdrachtgever.Find(id);
            if (t == null)
            {
                return "gefaald";
            }

            OU.opdrachtgever.Remove(t);
            OU.SaveChanges();

            return "gelukt";
        }
    }
}