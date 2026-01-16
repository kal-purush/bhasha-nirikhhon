using System;
using System.Collections.Generic;
using System.Text;

namespace SPS.AEM.Repository.Models
{
    public class CustomerDto
    {
        public int Id { get; set; }

        public string RRNumber { get; set; }

        public string FirstName { get; set; }

        public string LastName { get; set; }

        public int LoadTypeId { get; set; }

        public int TransformerId { get; set; }

        public string MobileNumber { get; set; }

        public int VillageId { get; set; }
    }
}