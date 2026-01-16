using Microsoft.AspNetCore.Mvc;
using dbcontext.Classes;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel;
using System.Reflection;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Linktindr_be.Controllers {
    [Route("api/[controller]")]
    [ApiController]
    public class SpecializationController : ControllerBase {
        // GET: api/<ValuesController>
        /*
        [HttpGet]
        public IEnumerable<string> Get() {
            return Enum.GetValues(typeof(Specialization)).Cast<Specialization>().Select(v => v.ToString()).ToList();
        }
        */
        [HttpGet]
        public Dictionary<string, string> GetSpecializations() {
            var specializations = Enum.GetValues(typeof(Specialization)).Cast<Specialization>();
            var dictionary = new Dictionary<string, string>();

            foreach(var specialization in specializations) {
                var fieldInfo = specialization.GetType().GetField(specialization.ToString());
                var descriptionAttribute = fieldInfo.GetCustomAttribute<DescriptionAttribute>();
                var description = descriptionAttribute != null ? descriptionAttribute.Description : specialization.ToString();

                dictionary.Add(specialization.ToString(), description);
            }
            return dictionary;
        }
    }
}