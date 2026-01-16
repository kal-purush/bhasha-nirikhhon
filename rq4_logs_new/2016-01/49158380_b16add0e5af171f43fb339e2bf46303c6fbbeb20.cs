using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.Composition;
using System.Linq;

using Rock.Model;
using System.Text.RegularExpressions;
using Rock.Data;
using System;

namespace Rock.Search.Person
{
    /// <summary>
    /// Searches for people with matching birthdays
    /// </summary>
    [Description("Person Birthday Search")]
    [Export(typeof(SearchComponent))]
    [ExportMetadata("ComponentName", "Person Birthday")]
    public class Birthday : SearchComponent
    {
        /// <summary>
        /// Returns a list of matching people
        /// </summary>
        /// <param name="searchterm"></param>
        /// <returns></returns>
        public override IQueryable<string> Search(string searchterm)
        {
            // is this is a valid date?
            DateTime birthDate;
            if (DateTime.TryParse(searchterm, out birthDate))
            {
                var personService = new PersonService(new RockContext());

                return personService.Queryable().
                    Where(p => p.BirthDate == birthDate).
                    OrderBy(p => p.BirthDate).
                    Select(p => p.BirthDate.ToString()).Distinct();
            }
            return null;            

        }
    }
}