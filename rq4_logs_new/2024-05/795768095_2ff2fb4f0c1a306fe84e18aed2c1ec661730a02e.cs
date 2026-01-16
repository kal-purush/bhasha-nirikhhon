using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary
{
    public class ManagerCall : ISubscription
    {
        public Subscription RegistrationSub(string type)
        {
            switch (type)
            {
                case "Domestic":
                    return new DomesticSubscription(35, 1, new List<string> { "Domestic" });
                case "Educational":
                    return new EducationalSubscription(30, 1, new List<string> { "Educational" });
                case "Premium":
                    return new PremiumSubscription(50, 12, new List<string> { "Premium" });
                default:
                    throw new ArgumentException("Invalid subscription type");
            }
        }
    }
}