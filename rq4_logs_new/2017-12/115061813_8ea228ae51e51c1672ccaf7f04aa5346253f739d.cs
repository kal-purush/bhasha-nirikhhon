using System.Collections.Generic;

namespace FOIapp.Classes
{
    public class Teacher
    {
        public int Id { get; set; }
        public string Name { get; set; }

        public string Surname { get; set; }

        public string FullNameNoTitle => $"{Name} {Surname}";

        public string Title { get; set; }

        public bool IsTitleAfterName { get; set; }

        public string FullName => IsTitleAfterName ? $"{Name} {Surname}, {Title}" : $"{Title} {Name} {Surname}";

        public string Email { get; set; }

        public string MailToLink => $"mailto:{Email}";

        public string ImageUrl { get; set; }

        public string OfficeLocation { get; set; }

        public string OfficeNumber { get; set; }

        public List<TeacherConsultations> Consultations { get; set; }
    }

}