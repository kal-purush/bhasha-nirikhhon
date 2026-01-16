using dbcontext;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Linktindr_be {
    [Route("api/[controller]")]
    [ApiController]
    public class SollicitatieController : ControllerBase {

        OurContext OC;

        public SollicitatieController(OurContext _OC) {
            this.OC = _OC;
        }

        // GET: api/<ValuesController>
        [HttpGet]
        public IEnumerable<Sollicitatie> Get()
        {
            return OC.sollicitaties.Select(s => new Sollicitatie
            {
                Id = s.Id,
                IdMedewerker = s.IdMedewerker,
                IdVacature = s.IdVacature,
                Status = s.Status,
                Medewerker_akkoord = s.Medewerker_akkoord,
                Werkgever_akkoord = s.Werkgever_akkoord
            }).ToList();
        }

        [HttpPost("add")]
        public string aanmaken(Sollicitatie_NoId sni)
        {
            Sollicitatie s = new Sollicitatie();

            s.IdMedewerker = sni.IdMedewerker;
            s.IdVacature = sni.IdVacature;
            s.Status = sni.Status;
            s.Medewerker_akkoord = sni.Medewerker_akkoord;
            s.Werkgever_akkoord = sni.Werkgever_akkoord;

            OC.Add(s);
            OC.SaveChanges();
            return "gelukt";
        }
    }
}