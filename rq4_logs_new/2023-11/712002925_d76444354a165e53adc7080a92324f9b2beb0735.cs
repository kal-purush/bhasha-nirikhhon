using FalknerCountyJuvenileCourt.Models;

namespace FalknerCountyJuvenileCourt.Data
{
    public static class DbInitializer
    {
        public static void Initialize(CourtContext context)
        {
            // Look for any students.
            if (context.Juveniles.Any())
            {
               return;   // DB has been seeded
            }

            var races = new Race[] {
               new Race{Name="Caucasian"},
               new Race{Name="African American"}
            };
            context.Races.AddRange(races);
            context.SaveChanges();

            var genders = new Gender[] {
               new Gender{Name=GEnum.Male},
               new Gender{Name=GEnum.Female}
            };
            context.Genders.AddRange(genders);
            context.SaveChanges();

            var risks = new Risk[] {
               new Risk{Name=REnum.Low},
               new Risk{Name=REnum.Medium},
               new Risk{Name=REnum.High},
               new Risk{Name=REnum.Unknown}
            };
            context.Risks.AddRange(risks);
            context.SaveChanges();

            var juveniles = new Juvenile[] {
               new Juvenile{Age=17,RaceID=0,GenderID=0,RiskID=0,Repeat=true},
               new Juvenile{Age=14,RaceID=1,GenderID=1,RiskID=4,Repeat=false}
            };
            context.Juveniles.AddRange(juveniles);
            context.SaveChanges();
        }
    }
}