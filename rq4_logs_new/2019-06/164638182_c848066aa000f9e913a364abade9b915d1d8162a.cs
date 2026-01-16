using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dfc.CourseDirectory.CourseMigrationTool.Helpers
{
    public static class FileHelper
    {
        //Get the list of providers to Migrate from the "migrationtoprovider.csv" stored ib BLOB storage
        //Provider	Date Migrated
        //10000001	08/06/2019
        //10000002	09/06/2019

        public static List<int> GetProviderUKPRNs(string filePath, string fileName, out string errorMessageGetCourses)
        {
            var providerUKPRNList = new List<int>();
            var count = 1;
            string errors = string.Empty;
            string ProviderSelectionsPath = string.Format(@"{0}", filePath);
            if (!Directory.Exists(ProviderSelectionsPath))
                Directory.CreateDirectory(ProviderSelectionsPath);
            string selectionOfProviderFile = string.Format(@"{0}\{1}", ProviderSelectionsPath, fileName);
            using (StreamReader reader = new StreamReader(selectionOfProviderFile))
            {
                string line = null;
                while (null != (line = reader.ReadLine()))
                {
                    try
                    {


                        string[] linedate = line.Split(',');


                        var provider = linedate[0];
                        var migrationdate = linedate[1];
                        DateTime migDate = DateTime.MinValue;
                        int provID = 0;
                        DateTime.TryParse(migrationdate, out migDate);
                        int.TryParse(provider, out provID);
                        if (migDate > DateTime.MinValue && migDate == DateTime.Today && provID > 0)
                        {
                            providerUKPRNList.Add(provID);
                        }
                    }
                    catch (Exception ex)
                    {
                        errors = errors + "Failed textract line: " + count.ToString() + "Ex: " + ex.Message;

                    }

                }
            }

            errorMessageGetCourses = errors;
            return providerUKPRNList;
        }
        public static List<int> GetProviderUKPRNsFromBlob(string filePath, string fileName, out string errorMessageGetCourses)
        {
            var providerUKPRNList = new List<int>();
            var count = 1;
            string errors = string.Empty;
            string ProviderSelectionsPath = string.Format(@"{0}", filePath);
            if (!Directory.Exists(ProviderSelectionsPath))
                Directory.CreateDirectory(ProviderSelectionsPath);
            string selectionOfProviderFile = string.Format(@"{0}\{1}", ProviderSelectionsPath, fileName);
            using (StreamReader reader = new StreamReader(selectionOfProviderFile))
            {
                string line = null;
                while (null != (line = reader.ReadLine()))
                {
                    try
                    {


                        string[] linedate = line.Split(',');


                        var provider = linedate[0];
                        var migrationdate = linedate[1];
                        DateTime migDate = DateTime.MinValue;
                        int provID = 0;
                        DateTime.TryParse(migrationdate, out migDate);
                        int.TryParse(provider, out provID);
                        if (migDate > DateTime.MinValue && migDate == DateTime.Today && provID > 0)
                        {
                            providerUKPRNList.Add(provID);
                        }
                    }
                    catch (Exception ex)
                    {
                        errors = errors + "Failed textract line: " + count.ToString() + "Ex: " + ex.Message;

                    }

                }
            }

            errorMessageGetCourses = errors;
            return providerUKPRNList;
        }
    }
}