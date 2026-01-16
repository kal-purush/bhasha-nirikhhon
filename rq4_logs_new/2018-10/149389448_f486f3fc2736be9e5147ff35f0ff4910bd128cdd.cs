using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MAJServices.Models
{
    public class FilePathDto
    {
        public int Id { get; set; }
        public string FileName { get; set; }
        public string Path { get; set; }
        public int PostId { get; set; }
    }
}