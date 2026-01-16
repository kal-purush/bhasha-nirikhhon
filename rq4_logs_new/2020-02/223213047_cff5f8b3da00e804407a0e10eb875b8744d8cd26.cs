using System.Collections.Generic;
using DFC.App.MatchSkills.Application.Session.Models;
using DFC.Personalisation.Domain.Models;

namespace DFC.App.MatchSkills.Models
{
    public class SkillSet : HashSet<Skill>
    {
        public void LoadFromSession(UserSession userSession)
        {
            if (null == userSession || null == userSession.Skills || 0 == userSession.Skills.Count)
            {
                this.Clear();
            }
            else
            {
                foreach (var skill in userSession.Skills)
                {
                    this.Add(new Skill(skill.Id, skill.Name));
                }
            }
        }
    }
}