using CivicdAPI.Models;
using Microsoft.AspNet.Identity.Owin;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;

namespace CivicdAPI.Controllers
{
    [Authorize]
    [RoutePrefix("api")]
    public class ActivityActionController : ApiController
    {

        [HttpPost]
        [Route("activities/{activityId:int}/rsvps/{userName}")]
        public bool RSVP(int activityId, string userName)
        {

            using (var context = new ApplicationDbContext())
            {
                var rsvp = new UserActivity();
                var activity = context.Activities.FirstOrDefault(a => a.ID == activityId);
                var user = context.Users.FirstOrDefault(u => u.UserName == userName);
                if (user == null)
                {
                    throw new Exception("Invalid Username");
                }
                if (activity == null)
                {
                    throw new Exception("Invalid Activity Id");
                }
                
                rsvp.Activity = activity;
                rsvp.User = user;

                context.UserActivities.Add(rsvp);
                context.SaveChanges();

                return true;
            }
        }

        [HttpPost]
        [Route("activities/{activityId:int}/checkins/{userName}")]
        public bool CheckInActivity(int activityId, string userName)
        {
            using (var context = new ApplicationDbContext())
            {
                var user = context.Users.FirstOrDefault(u => u.UserName == userName);
                if(user == null)
                {
                    throw new Exception("Invalid Username");
                }

                var activity = context.UserActivities.FirstOrDefault(ua => ua.ActivityID == activityId && ua.User.Id == user.Id);
                if(activity == null)
                {
                    throw new Exception("Invalid Activity or User is Not RSVP'd for this Activity");
                }

                activity.CheckedIn = true;
                context.SaveChanges();
                return true;
            }
        }

        [HttpPost]
        [Route("activities")]
        public ActivityViewModel CreateActivity(ActivityViewModel activity)
        {
            using (var context = new ApplicationDbContext())
            {
                //Todo: Check security to ensure org is creating activity.
                var activityEntity = new Activity()
                {
                    DisplayTitle = activity.DisplayTitle,
                    Category = activity.CategoryName,
                    StartTime = activity.StartTime,
                    EndTime = activity.EndTime,

                };

                if (!String.IsNullOrEmpty(activity.StreetAddressOne))
                {
                    activityEntity.Address = new Address()
                    {
                        StreetAddressOne = activity.StreetAddressOne,
                        StreetAddressTwo = activity.StreetAddressTwo,
                        City = activity.City,
                        State = activity.State,
                        ZipCode = activity.ZipCode,
                        Name = activity.AddressDisplayName
                    };
                }

                context.Activities.Add(activityEntity);
                context.SaveChanges();
                activity.Id = activityEntity.ID;

                return activity;
            }
        }

        [HttpPut]
        [Route("activities/{activityId:int}")]
        public ActivityViewModel UpdateActivity(int activityId, ActivityViewModel activity)
        {
            using (var context = new ApplicationDbContext())
            {
                var activityEntity = context.Activities.FirstOrDefault(a => a.ID == activityId);
                if (activityEntity == null)
                {
                    throw new Exception("Unable to Find Matching Activity");
                }

                activityEntity.DisplayTitle = activity.DisplayTitle;
                activityEntity.Category = activity.CategoryName;
                activityEntity.StartTime = activity.StartTime;
                activityEntity.EndTime = activity.EndTime;
                if (!String.IsNullOrEmpty(activity.StreetAddressOne))
                {
                    if (activityEntity.Address == null)
                    {
                        activityEntity.Address = new Address()
                        {
                            StreetAddressOne = activity.StreetAddressOne,
                            StreetAddressTwo = activity.StreetAddressTwo,
                            City = activity.City,
                            State = activity.State,
                            ZipCode = activity.ZipCode,
                            Name = activity.AddressDisplayName
                        };
                    }
                    else
                    {
                        activityEntity.Address.StreetAddressOne = activity.StreetAddressOne;
                        activityEntity.Address.StreetAddressTwo = activity.StreetAddressTwo;
                        activityEntity.Address.City = activity.City;
                        activityEntity.Address.State = activity.State;
                        activityEntity.Address.ZipCode = activity.ZipCode;
                        activityEntity.Address.Name = activity.AddressDisplayName;
                    }
                }

                context.SaveChanges();
                return activity;
            }
        }

        [HttpDelete]
        [Route("activities/{activityId:int}")]
        public bool DeleteActivity(int activityId)
        {
            using (var context = new ApplicationDbContext())
            {
                var activityEntity = context.Activities.FirstOrDefault(a => a.ID == activityId);
                if (activityEntity == null)
                {
                    throw new Exception("Unable to Find Matching Activity");
                }

                context.Activities.Remove(activityEntity);

                context.SaveChanges();
                return true;
            }
        }
    }
}