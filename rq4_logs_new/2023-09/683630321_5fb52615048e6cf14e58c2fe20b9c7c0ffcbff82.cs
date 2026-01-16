namespace Common.Entities
{
    public class ApplicationUserDetails
    {
        public int UserId { get; private set; }
        public string ImagePath { get; private set; }
        public string FirstName { get; private set; }
        public string LastName { get; private set; }
        public string AddressLine1 { get; private set; }
        public string AddressLine2 { get; private set; }
        public string City { get; private set; }
        public string StateCode { get; private set; }
        public string PostCode { get; private set; }
        public string Mobile { get; private set; }
        public string AlternateEmail { get; private set; }
        public string CountryCode { get; private set; }
        public string AlternateMobile { get; private set; }
        public DateTime CreateDate { get; private set; }
        public DateTime ModifiedDate { get; private set; }

        public ApplicationUser User { get; set; }

        protected ApplicationUserDetails()
        {
        }

        public ApplicationUserDetails
            (
            int userId,
            string imagePath,
            string firstName,
            string lastName,
            string addressLine1,
            string addressLine2,
            string city,
            string stateCode,
            string postCode,
            string mobile,
            string alternateEmail,
            string countryCode,
            string alternateMobile
            )
        {
            UserId = userId;
            ImagePath = imagePath;
            FirstName = firstName;
            LastName = lastName;
            AddressLine1 = addressLine1;
            AddressLine2 = addressLine2;
            City = city;
            StateCode = stateCode;
            PostCode = postCode;
            Mobile = mobile;
            AlternateEmail = alternateEmail;
            CountryCode = countryCode;
            AlternateMobile = alternateMobile;
            CreateDate = DateTime.UtcNow;
            ModifiedDate = DateTime.UtcNow;
        }


    }
}