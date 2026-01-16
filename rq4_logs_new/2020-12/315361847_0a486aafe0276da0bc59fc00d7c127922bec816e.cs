using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Mvc;


namespace BookLib.Controllers
{
    public class Student
    {
        public string Name { get; set; }
        public string Id { get; set; }
    }


    [Route("api/[controller]")]
    [ApiController]
    public class ValuesController : ControllerBase
    {
        private readonly IDataProtectionProvider _dataProtectionProvider;
        private List<Student> students = new List<Student>();

        public ValuesController(IDataProtectionProvider dataProtectionProvider)
        {
            _dataProtectionProvider = dataProtectionProvider;
            students.Add(new Student
            {
                Id = "1",
                Name = "Tony"
            });
            students.Add(new Student
            {
                Id = "2",
                Name = "Jack"
            });
        }

        [HttpGet]
        public ActionResult<IEnumerable<Student>> Get()
        {
            var protector = _dataProtectionProvider.CreateProtector("ProtectResourceId");
            var result = students.Select(s => new Student
            {
                Id = protector.Protect(s.Id),
                Name = s.Name
            });

            return result.ToList();
        }

        [HttpGet("{id}")]
        public ActionResult<Student> Get(string id)
        {
            var protector = _dataProtectionProvider.CreateProtector("ProtectResourceId");
            var rawId = protector.Unprotect(id);
            var targetItem = students.FirstOrDefault(s => s.Id == rawId);
            return new Student { Id = id, Name = targetItem.Name };
        }
    }
}