using dbcontext;
using dbcontext.Classes;
using Linktindr_be.Dto;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;


// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Linktindr_be.Controllers {
    [Route("api/[controller]")]
    [ApiController]
    public class SkillController : ControllerBase {

        private readonly OurContext OC;

        public SkillController(OurContext OC) {
            this.OC = OC;
        }

        // GET: api/<SkillController>
        [HttpGet]
        public List<SkillDto> Get() {
            return OC.Skill.Include(s => s.Medewerker)
                .Select(s => new SkillDto(s))
                .ToList();
        }

        // GET api/<SkillController>/5
        [HttpGet("{id}")]
        public SkillDto? Get(int id) {
            // Haal eerst de medewerker op. Met de ? geven we aan dat die null kan zijn
            Skill? s = OC.Skill.Include(s => s.Skills).FirstOrDefault(s => s.Id == id);

            // Als die null is dan is die niet gevonden dus dan kunnen we ook null 
            // terug geven. Dan is de response body leeg. 
            if(s == null)
                return null;

            // Hier is de medewerker dus bekend en die kan je omzetten
            return new SkillDto(s);
        }
        //GET (maak nieuwe skill aan) api/<SkillController>/add
        [HttpPost("add")]
        public bool Add(SaveSkillDto dto) {
            // Haal de medewerker op. 
            Medewerker? m = OC.Medewerker.Find(dto.MedewerkerId);

            // Als die niet gevonden kan worden dan geven we false terug. Namelijk je
            // wilt ervoor zorgen dat alleen bestaande foreign keys worden gebruikt
            if(m == null)
                return false;

            Skill s = new Skill();
            s.Medewerker = m;
            s.Language = Enum.Parse<Language>(dto.Language);
            s.Level = Enum.Parse<Level>(dto.Level);
            s.Title = dto.Title;
            s.Description = dto.Description;

            OC.Add(s);
            OC.SaveChanges();
            return true;
        }
    }
}