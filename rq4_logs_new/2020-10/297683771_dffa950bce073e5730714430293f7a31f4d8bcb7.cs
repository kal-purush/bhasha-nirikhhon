using latex_curriculum_vitae.Models;
using latex_curriculum_vitae.Services;
using System;
using System.IO;

namespace latex_curriculum_vitae
{
    public static class JobApplicationCSV
    {
        public static void WriteCSV(string company, string jobtitle, string city, string joburl)
        {
            string appDataPath = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
            string srcPath = Path.Combine(appDataPath, "latex_curriculum_vitae");

            var csvParserService = new CsvParserService();
            var path = Path.Combine(srcPath, "JobApplications.csv");
            var result = csvParserService.ReadCsvFileToJobApplicationModel(path);

            var jobApplicationToAdd = new JobApplicationModel()
            {
                Company = company,
                Jobtitle = jobtitle,
                City = city,
                Status = "Email sent",
                EmailSent = DateTime.Today,
                JobOfferUrl = joburl
            };

            result.Add(jobApplicationToAdd);
            csvParserService.WriteNewCsvFile(path, result);
        }
    }
}