using NewsPaper.Journal;
using NewsPaper.Subscriber;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NewsPaper.Adapter
{
    public class AdapteeService
    {
        public SubscriberInfo Convert(PersonalInformation personalInformation , int id)
        {
            SubscriberInfo subscriberInfo = new SubscriberInfo();
            subscriberInfo.FullName = personalInformation.Name + " " + personalInformation.Family;
            subscriberInfo.BirthDate = new string(personalInformation.BirthDate.Reverse().ToArray());
            subscriberInfo.Gender = (int)personalInformation.Gender;
            subscriberInfo.Id = id;

            return subscriberInfo;
        }
    }
}