using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Web;

namespace ProjectFarmerAPI.Models
{
    public class Entity
    {
        /// <summary>
        /// The Entity Id
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// The Entity First Name
        /// </summary>
        [Required]
        public string FirstName { get; set; }

        /// <summary>
        /// The Customers Last Name
        /// </summary>
        public string LastName { get; set; }

        /// <summary>
        /// The Mobile Number of the Entity, It can be different from the Address. We can use this same number for the Address as well.
        /// </summary>
        [Required]
        public string MobileNumber { get; set; }

        /// <summary>
        /// The Secondary Phone Number
        /// </summary>
        public string PhoneNumber { get; set; }

        /// <summary>
        /// Email Address of the Entity
        /// </summary>
        public string EmailAddress { get; set; }

        /// <summary>
        /// The Prefered Address of the Entity  
        /// </summary>
        [Required]
        public Address PreferedAddress { get; set; }

        /// <summary>
        /// The Other Addresses of the Entity, null in case of no other address
        /// </summary>
        public IEnumerable<Address> Addresses { get; set; }

        /// <summary>
        /// This is very private information about the Entity and it will not be accesable by outsiders
        /// </summary>
        protected double EntityRating { get; private set; }
    }
}