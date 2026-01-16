using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PsicopataPedidos.Domain.Models
{
     public class UserAD
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Email { get; set; }
        public string TenantId { get; set; }
    }
}