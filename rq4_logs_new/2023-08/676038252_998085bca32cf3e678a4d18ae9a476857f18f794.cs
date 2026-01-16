using System;
using FacebookWrapper.ObjectModel;

namespace BasicFacebookFeatures.Features.RelationshipFeature
{
    public static class RelationshipFeature
    {
        public static User SelectedFriend { get; set; }
        public static User.eGender GenderPreference { get; set; }
        public static int MinAgePreference { get; set; }    // probably should check that age < 18 is not allowed if user is above 18 //
        public static int MaxAgePreference { get; set; }
        public static bool SameHomeTownLimitPreference { get; set; }     // True if user wants to match with people from the same hometown //

        public static FacebookObjectCollection<User> FindMatchesBasedOnPreferences()
        {
            // maybe need to add here a check if all preferences are selected //
            
            FacebookObjectCollection<User> matches = new FacebookObjectCollection<User>();

            foreach (User possibleMatchUser in SelectedFriend.Friends)
            {
                if(checkIfMatchPreferencesAreMet(possibleMatchUser))
                {
                    matches.Add(possibleMatchUser);
                }
            }
            
            return matches;
        }

        private static bool checkIfMatchPreferencesAreMet(User i_PossibleMatch)
        {
            bool isSingle = i_PossibleMatch.RelationshipStatus == User.eRelationshipStatus.Single;
            bool isPreferencedGender = i_PossibleMatch.Gender == GenderPreference;
            bool isHomeTownPreferenceConditionMet = !SameHomeTownLimitPreference || i_PossibleMatch.Hometown.Name == SelectedFriend.Hometown.Name;
            bool isAgePreferenceMet = checkIfAgePreferenceIsMet(i_PossibleMatch);
            
            return isSingle && isPreferencedGender && isHomeTownPreferenceConditionMet && isAgePreferenceMet;
        }
        
        private static bool checkIfAgePreferenceIsMet(User i_PossibleMatch)
        {
            bool isAgePreferenceMet = false;
            
            if(i_PossibleMatch.Birthday != null)
            {
                DateTime birthday = DateTime.Parse(i_PossibleMatch.Birthday);
                int age = DateTime.Today.Year - birthday.Year;
                isAgePreferenceMet = age >= MinAgePreference && age <= MaxAgePreference;
            }
            
            return isAgePreferenceMet;
        }
    }
}