namespace ActivityFinder.Server.Models
{
    public static class ActivityMapper
    {
        public static Activity ToActivity(this ActivityDTO activityDTO, Address address, ApplicationUser user)
        {
            var activity = new Activity()
            {
                ActivityId = activityDTO.Id ?? 0,
                Description = activityDTO.Description!,
                Date = activityDTO.Date!.Value,
                OtherInfo = activityDTO.OtherInfo,
                Address = address,
                Creator = user               
            };
            return activity;
        }
    }
}