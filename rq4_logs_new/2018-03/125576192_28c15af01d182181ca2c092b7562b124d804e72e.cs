using System.Collections.Generic;    
using Nikita.Lib.Data;
using Nikita.Lib.Domain;
using Nikita.Lib.Interfaces;

namespace DataConverter.Commands
{
   public class GetPersonDataCommand : BaseGetDataCommand<Person>
   {
       private PersonService _service;
        public GetPersonDataCommand(PersonService service) : base()
        {
            _service = service;
        }       
        protected override IEnumerable<Person> RunQuery()
        {
            return _service.GetAll();
          }
    }
}