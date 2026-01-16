using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FuglariApi.Models
{
    public class LandmarkDto
    {
        public int Id { get; set; }
        public int DatasetId { get; set; }
        public string DatasetTitle { get; set; }
        public float Longitude { get; set; }
        public DateTime DateCreated { get; set; }
        public float Latitude { get; set; }
        public int Status { get; set; }
        public int Progress { get; set; }

    }
}