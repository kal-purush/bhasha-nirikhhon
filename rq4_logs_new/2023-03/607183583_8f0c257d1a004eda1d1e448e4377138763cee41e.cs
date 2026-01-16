using dbcontext.Classes;

namespace Linktindr_be.Dto
{
    public class MedewerkerDto
    {
        public int Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public DateTime DateOfBirth { get; set; }
        public string PostCode { get; set; }
        public string HouseNumber { get; set; }
        public int Telephone { get; set; }
        public int Radius { get; set; }
        public Specialization Uitstroomrichting { get; set; } = Specialization.DevOps;
        public string Photo { get; set; }
        public string ProfileText { get; set; }
        public int TalentManagerId { get; set; }

        public string TalentManagerName { get; set; }

        public MedewerkerDto()
        {

        }
        public MedewerkerDto(Medewerker medewerker)
        {
            this.Id = medewerker.Id;
            this.FirstName = medewerker.FirstName;
            this.LastName = medewerker.LastName;
            this.DateOfBirth = medewerker.DateOfBirth;
            this.PostCode = medewerker.PostCode;
            this.HouseNumber = medewerker.HouseNumber;
            this.Telephone = medewerker.Telephone;
            this.Radius = medewerker.Radius;
            this.Uitstroomrichting = medewerker.Uitstroomrichting;
            this.Photo = medewerker.Photo;
            this.ProfileText = medewerker.ProfileText;
            this.TalentManagerId = medewerker.TalentManager.Id;
            this.TalentManagerName = medewerker.TalentManager.FirstName + " " + medewerker.TalentManager.LastName;
        }
    }
}