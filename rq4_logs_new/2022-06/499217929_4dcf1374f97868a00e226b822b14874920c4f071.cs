using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FuglariApi.Models
{
    public class ProjectDto
    {
        public int Id { get; set; }
        public string Title { get; set; }
        public IEnumerable<User> Members { get; set; }
        public IEnumerable<DatasetDto> Datasets { get; set; }
    }
}