using System;
using System.IO;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace ClassScheduleCalendar
{
    public class Storage
    {
        private string _path;

        public Storage()
        {
            this._path = Environment.GetEnvironmentVariable("STORAGE_LOCATION");
        }

        public void Save(string fileName, ClassSchedule schedule)
        {
            string jsonOuput = JsonConvert.SerializeObject(schedule);

            using (System.IO.StreamWriter file =
                new System.IO.StreamWriter(_path + "/" + fileName))
            {
                file.WriteLine(jsonOuput);
            }
        }

        public ClassSchedule Load(string fileName)
        {
            ClassSchedule schedule;
            using (StreamReader reader = new StreamReader(_path + "/" + fileName))
            {
                string json = reader.ReadToEnd();
                schedule = JsonConvert.DeserializeObject<ClassSchedule>(json);
            }

            return schedule;
        }

        public bool Exist(string fileName)
        {
            return File.Exists(_path + "/" + fileName);
        }
    }
}