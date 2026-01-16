using HelpI.API.Security.Domain.Models;

namespace HelpI.API.Application.Domain.Models
{
    public class ExpertApplication
    {
        public int Id { get; set; }
        public ApplicationForm ApplicationForm { get; set; }
        public int GameId { get; set; }
        public int PlayerId { get; set; }
        public Player Player { get; set; }

        public void SetApplicationForm(ApplicationForm applicationDetails, EApplicationStatus reviewStatus, string reviewComment)
        {
            this.ApplicationForm = new ApplicationForm(applicationDetails.Description,
                applicationDetails.VideoApplication, reviewStatus, reviewComment);
        }

        public void SetApplicationForm(ApplicationForm applicationForm)
        {
            this.ApplicationForm = applicationForm;
        }
    }
}