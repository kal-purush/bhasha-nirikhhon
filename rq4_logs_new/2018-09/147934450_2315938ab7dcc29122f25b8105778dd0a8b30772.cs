using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Tapir.Core;

namespace Tapir.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PersonsController : ControllerBase
    {
        private IPersonService personService;

        public PersonsController(IPersonService personService)
        {
            this.personService = personService;
        }

        [HttpGet]
        [ProducesResponseType(200)]
        public ActionResult<List<PersonDTO>> GetAll()
        {
            List<PersonDTO> persons = personService.GetPersons();
            return persons;
        }

        [HttpGet("{id}", Name = "GetPerson")]
        [ProducesResponseType(200)]
        [ProducesResponseType(404)]
        public ActionResult<PersonDTO> GetById(int id)
        {
            PersonDTO person = personService.GetPerson(id);
            if (person == null)
            {
                return NotFound();
            }
            return person;
        }

        [HttpPost]
        [ProducesResponseType(200)]
        [ProducesResponseType(400)]
        public ActionResult<PersonDTO> Create(PersonDTO person)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            PersonDTO newPerson = personService.SavePerson(person);
            return CreatedAtRoute(routeName: "GetPerson", routeValues: new { id = newPerson.ID }, value: newPerson);
        }

        [HttpPut("{id}")]
        [ProducesResponseType(204)]
        [ProducesResponseType(400)]
        [ProducesResponseType(404)]
        public ActionResult<PersonDTO> Update(int id, PersonDTO person)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            PersonDTO updatedPerson = personService.SavePerson(person);
            if (updatedPerson == null)
            {
                return NotFound();
            }
            return NoContent();
        }

        [HttpDelete("{id}")]
        [ProducesResponseType(204)]
        [ProducesResponseType(404)]
        public ActionResult<PersonDTO> Delete(int id)
        {
            PersonDTO deletedPerson = personService.RemovePerson(id);
            if (deletedPerson == null)
            {
                return NotFound();
            }
            return NoContent();
        }
    }
}