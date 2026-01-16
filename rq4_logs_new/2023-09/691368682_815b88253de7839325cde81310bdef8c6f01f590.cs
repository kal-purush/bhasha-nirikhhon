using Application.DTO.Response;
using Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.DTO.Request
{
    public class CompanyRequest
    {
        public required string Cuit { get; set; }
        public required string Name { get; set; }
        public required string Adress { get; set; }
        public required string Phone { get; set; }
    }
}