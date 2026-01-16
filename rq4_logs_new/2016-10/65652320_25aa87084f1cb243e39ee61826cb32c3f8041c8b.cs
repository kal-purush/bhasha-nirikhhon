using InspurOA.DAL;
using InspurOA.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace InspurOA.BLL
{
    public class StatisticsBLL
    {
        InspurDbContext dal = new InspurDbContext();

        public int GetResumeTotalCount()
        {
            return dal.ResumeSet.Count();
        }


        public List<StatisticsModel> GetResumeCountGroupByProjectName()
        {
            List<StatisticsModel> list = new List<StatisticsModel>(); ;
            var query = from r in dal.ResumeSet
                        group r by r.ProjectName into g
                        select new
                        {
                            ProjectName = g.FirstOrDefault().ProjectName,
                            Count = g.Sum(a => 1)
                        };

            foreach (var item in query)
            {
                if (item != null)
                {
                    list.Add(new StatisticsModel { ProjectName = item.ProjectName, Count = item.Count });
                }
            }

            return list;
        }
    }
}