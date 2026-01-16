using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CommentSys.Models
{

    public class ErrorResponse
    {
        public string code { get; set; }
        public string message { get; set; }
        public string subCode { get; set; }
        public string subMessage { get; set; }
    }

}