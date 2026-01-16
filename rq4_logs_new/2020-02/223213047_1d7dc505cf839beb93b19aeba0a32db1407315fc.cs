using DFC.App.MatchSkills.Models;
using DFC.App.MatchSkills.ViewModels;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace DFC.App.MatchSkills.Controllers
{
    public class MoreSkillsController : CompositeSessionController<MoreSkillsCompositeViewModel>
    {
        public MoreSkillsController(IDataProtectionProvider dataProtectionProvider, IOptions<CompositeSettings> compositeSettings) 
            : base(dataProtectionProvider, compositeSettings)
        {
        }

        [HttpPost]
        [Route("MatchSkills/body/[controller]")]
        public IActionResult Body(MoreSkills choice)
        {
            switch (choice)
            {
                case MoreSkills.Job:
                    return RedirectPermanent($"{base.ViewModel.CompositeSettings.Path}/{CompositeViewModel.PageId.OccupationSearch}");
                case MoreSkills.Skill:
                default:
                    return RedirectPermanent($"{base.ViewModel.CompositeSettings.Path}/{CompositeViewModel.PageId.MoreSkills}");
            }
        }
    }
}