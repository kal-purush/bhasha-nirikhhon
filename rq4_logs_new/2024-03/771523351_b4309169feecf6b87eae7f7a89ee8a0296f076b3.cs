using SomerenDAL;
using SomerenModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SomerenService
{
    public class ActivityService
    {
        private TeacherDao teacherDao;
        private ActivityDao activityDao;

        public ActivityService()
        {
            teacherDao = new TeacherDao();
            activityDao = new ActivityDao();
        }

        public List<Activity> GetAllActivities()
        {
            return activityDao.GetAllActivities();
        }

        public Activity GetActivityById(int id)
        {
            foreach (Activity activity in GetAllActivities())
            {
                if (activity.ActivityId == id)
                {
                    return activity;
                }
            }

            return null;
        }

        public List<ActivitySupervisor> GetAllActivitySupervisors()
        {
            TeacherService teacherService = new TeacherService();
            List<ActivitySupervisor> activitySupervisors = new List<ActivitySupervisor>();

            foreach (ActivitySupervisor activitySupervisor in activityDao.GetAllActivitiesSupervisors())
            {
                activitySupervisor.Activity = GetActivityById(activitySupervisor.ActivityID);
                activitySupervisor.Teacher = teacherService.GetTeacherById(activitySupervisor.LecturerID);
                activitySupervisors.Add(activitySupervisor);
            }

            return activitySupervisors;
        }

        public void AddActivitySupervisor(ActivitySupervisor activitySupervisor)
        {
            activityDao.AddActivitySupervisor(activitySupervisor);
        }

        public void RemoveActivitySupervisor(ActivitySupervisor activitySupervisor)
        {
            activityDao.RemoveActivitySupervisor(activitySupervisor);
        }
    }
}