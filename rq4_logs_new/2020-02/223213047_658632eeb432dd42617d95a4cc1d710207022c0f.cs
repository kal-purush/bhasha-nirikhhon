using DFC.Personalisation.Domain.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using DFC.App.MatchSkills.Application.Session.Models;

namespace DFC.App.MatchSkills.Models
{
    [Serializable]
    public class OccupationSet : HashSet<Occupation>
    {
        public void LoadFromSession(UserSession userSession)
        {
            if (userSession?.Occupations == null || userSession.Occupations.Count == 0)
            {
                this.Clear();
            }
            else
            {
                foreach (var occupation in userSession.Occupations)
                {
                    this.Add(new Occupation(occupation.Id, occupation.Name, occupation.DateAdded));
                }
            }
        }
    }
}