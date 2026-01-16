using System;
using System.Configuration;
using TennisClub.AppCore.interfaces;
using TennisClub.AppCore.model.interfaces;

namespace TennisClub.AppCore.validators
{
    public class IsAgeAllowAddChildValidator : IPairValidator<IChild, ICachedGroup>
    {
        private readonly int maxAgeInterval;
        public IsAgeAllowAddChildValidator()
        {
            this.maxAgeInterval = Convert.ToInt32(
                    ConfigurationManager.AppSettings.Get("maxChildrenAgeIntervalInGroup"));
        }

        public bool Validate(IChild child, ICachedGroup group)
        {
            int age = child.Age;
            return (age >= group.MinAge && age <= group.MaxAge)
                || age - group.MinAge <= maxAgeInterval
                || group.MaxAge - age <= maxAgeInterval;
        }
    }
}