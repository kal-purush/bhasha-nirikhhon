using madHealth.Database;
using madHealth.Repository;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace madHealth.Seed
{
    public class AnalysisResults
    {

        public static void Get()
        {
            ImadHealthRepository<AnalysisResult> analysisresults = new madHealthRepository<AnalysisResult>(Help.Context);
            ImadHealthRepository<AnalysisType> analysistypes = new madHealthRepository<AnalysisType>(Help.Context);                  
            DataTable rawData = Help.OpenExcel("AnalysisResults");
            int N = 1;
            foreach (DataRow row in rawData.Rows)
            {
                int oldId = Help.getInteger(row, 0);
                int typeID = Help.getInteger(row, 4);                 
                AnalysisType atype = analysistypes.Get().FirstOrDefault(x => x.Id == typeID);                     
                AnalysisResult result = new AnalysisResult()
                {
                    Title = Help.getString(row, 1),
                    Type = atype,
                    Description = Help.getString(row,3),
                    Status = (AnalysisStatus)Help.getInteger(row, 2),                    
                };
                N++;
                analysisresults.Insert(result);
                analysisresults.Commit();
                Help.dicARes.Add(oldId, result.Id);
            }
            Console.WriteLine(N);
        } 
    }
}