using System;
using System.Collections.Generic;
using System.Linq;
using WebResume.Domain;

namespace WebResume.Data
{
    public interface IExperienceData
    {
        IEnumerable<Experience> GetAll();
    }
    
    public class ExperienceData : IExperienceData
    {
        private IEnumerable<Experience> _experiences;

        public ExperienceData()
        {
            _experiences = new List<Experience>
            {
                new Experience
                {
                    Company = "Deloitte",
                    Title = "Quality Assurance Analyst - Team Lead",
                    Duration = new Tuple<string, string>("August 2018", "September 2019"),
                    Responsibilities = new List<string>
                    {
                        "QA representative for project’s UAT - responsible for troubleshooting defects and being the liaison between the business and QA Team ",
                        "Create Postman collections to automate smoke tests",
                        "Utilizing SSMS to validate the integrity of the data in the database"
                    }
                },
                new Experience
                {
                    Company = "AgileThought",
                    Title = "Jr Information Technology Associate",
                    Duration = new Tuple<string,string>("August 2017", "August 2018"),
                    Responsibilities = new List<string>
                    {
                        "Responsible for troubleshooting and debugging front-end and back-end work",
                        "Facilitated defect triaging meetings with development team",
                        "Strategically tested software using Swagger and Postman"
                    }
                },
                new Experience
                {
                    Company = "ConnectOne Bank",
                    Title = "IT Intern",
                    Duration = new Tuple<string,string>("May 2015","August 2015"),
                    Responsibilities = new List<string>
                    {
                        "Managed the setup and installation of remote access laptops for company employees",
                        "Perform mainframe and account maintenance tasks",
                        "Assisted in installation and integration during server overhauls"
                    }
                }
            };
        }
        public IEnumerable<Experience> GetAll()
        {
            return from r in _experiences
                select r;
        }
    }
}