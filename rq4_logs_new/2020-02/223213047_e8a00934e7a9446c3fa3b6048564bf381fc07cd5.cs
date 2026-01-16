using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DFC.App.MatchSkills.ViewModels;

namespace DFC.App.MatchSkills.ViewComponents.OccupationSearchScript
{
    public class OccupationSearchScriptModel
    {
        public string SearchService { get; set; }
        public string AutoCompleteElementName { get; set; }
        public string FormElementName { get; set; }
        public string SearchButton { get; set; }
        public string AutoCompleteError { get; set; }
    }
}