using EventPlanner.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace EventPlanner.Services
{
    class SeatingPlanService
    {
        private readonly string PATH = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), @"Data\seatingplans.json");
        private static SeatingPlanService singleton = null;


        public static SeatingPlanService Singleton()
        {
            return singleton ??= new SeatingPlanService();
        }

        public SeatingPlan GetseatingPlanInfo(int id)
        {
            return GetseatingPlans().FirstOrDefault(seatingPlan => seatingPlan.Id == id);
        }

        public List<SeatingPlan> GetseatingPlans()
        {
            List<SeatingPlan> seatingPlans = new List<SeatingPlan>();
            using (StreamReader reader = new StreamReader(PATH))
            {
                string data = reader.ReadToEnd();
                seatingPlans = JsonConvert.DeserializeObject<List<SeatingPlan>>(data);
            }
            return seatingPlans;
        }

        public SeatingPlan GetseatingPlanByTask(int id)
        {
            return GetseatingPlans().FirstOrDefault(el => el.TaskId == id);
        }

        public List<SeatingPlan> Delete(SeatingPlan seatingPlan)
        {
            List<SeatingPlan> seatingPlans = GetseatingPlans();
            seatingPlans.RemoveAll(el => el.Id == seatingPlan.Id);
            save(seatingPlans);
            return seatingPlans;
        }

        public List<SeatingPlan> Modify(SeatingPlan seatingPlan)
        {
            List<SeatingPlan> seatingPlans = GetseatingPlans();
            for (int i = 0; i < seatingPlans.Count(); i++)
            {
                if (seatingPlans[i].Id == seatingPlan.Id)
                {
                    seatingPlans[i] = seatingPlan;
                }
            }
            save(seatingPlans);
            return seatingPlans;
        }

        public List<SeatingPlan> Add(SeatingPlan seatingPlan)
        {
            List<SeatingPlan> seatingPlans = GetseatingPlans();
            seatingPlans.Add(seatingPlan);
            save(seatingPlans);
            return seatingPlans;
        }

        public int GetLastId()
        {
            List<SeatingPlan> seatingPlans = GetseatingPlans();
            int greatest = 1;
            foreach (var seatingPlan in seatingPlans)
            {
                if (greatest < seatingPlan.Id)
                {
                    greatest = seatingPlan.Id;
                }
            }
            return ++greatest;
        }

        public void save(List<SeatingPlan> seatingPlans)
        {
            using (StreamWriter writer = new StreamWriter(PATH))
            {
                string data = JsonConvert.SerializeObject(seatingPlans);
                writer.WriteLine(data);
            }
        }
    }
}