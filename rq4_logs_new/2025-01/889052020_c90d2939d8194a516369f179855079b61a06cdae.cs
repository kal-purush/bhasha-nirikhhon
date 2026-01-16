using EmailManager.Client.Enum;
using Google.Apis.Gmail.v1;
using Microsoft.Graph;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EmailManager.Client.Model
{
    public class AuthenticatedAccount
    {
        public string Email { get; set; }
        public string DisplayName { get; set; }
        public Provider Provider { get; set; } 
    }

}