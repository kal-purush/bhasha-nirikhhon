using System;
using System.Configuration;
using TennisClub.AppCore.model.impl;
using TennisClub.AppCore.model.interfaces;

namespace TennisClub.AppCore.validators
{
    public class ChildAgeRuleChecker
    {
        private readonly int maxAgeInterval;
        
        public ChildAgeRuleChecker()
        {
            this.maxAgeInterval = Convert.ToInt32(
                ConfigurationManager.AppSettings.Get("maxChildrenAgeIntervalInGroup") ?? "3");
        }

        public Func<int, int, bool> CreateRuleCheckerDelegate(Child child)
        {
            int age = child.Age;
            Func<int, int, bool> fitsInInterval = (minAge, maxAge) =>  
                age >= minAge && age <= maxAge;
            Func<int, int, bool> willFitInInterval = (minAge, maxAge) => 
                (age - minAge <= maxAgeInterval) && (maxAge - age <= maxAgeInterval);
            return (minAge, maxAge) => 
                fitsInInterval(minAge, maxAge) || willFitInInterval(minAge, maxAge);
        }
    }
}