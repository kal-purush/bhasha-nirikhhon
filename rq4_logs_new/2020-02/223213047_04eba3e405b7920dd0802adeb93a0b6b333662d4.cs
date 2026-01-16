using DFC.App.MatchSkills.Models;
using DFC.App.MatchSkills.ViewModels;
using DFC.Personalisation.Domain.Models;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace DFC.App.MatchSkills.Controllers
{
    public class MatchesController : CompositeSessionController<MatchesCompositeViewModel>
    {
        public MatchesController(IDataProtectionProvider dataProtectionProvider, IOptions<CompositeSettings> compositeSettings)
            : base(dataProtectionProvider, compositeSettings)
        {
        }

        public override IActionResult Body()
        {
            var cm = new CareerMatch()
            {
                JobSectorGrowthDescription = "Increasing",
            };
            cm.JobProfile.Title = "Job Title of First Match";
            cm.JobProfile.Description = "Here is a description of the job profile.";
            cm.MatchedSkills.Add(new Skill("fm1", "First matched skill", SkillType.Competency));
            cm.MatchedSkills.Add(new Skill("fm2", "Second  matched skill", SkillType.Competency));
            cm.MatchedSkills.Add(new Skill("fm3", "Third matched skill", SkillType.Competency));
            cm.UnMatchedSkills.Add(new Skill("um1", "First unmatched skill", SkillType.Competency));

            ViewModel.CareerMatches.Add(cm);

            return base.Body();
        }

    }
}