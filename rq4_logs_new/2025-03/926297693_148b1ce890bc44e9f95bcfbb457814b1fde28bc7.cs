using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPSS.Service.Dto.Request
{
    public class ProductRequestUpdate
    {
        public string? BrandName { get; set; }
        public string? CategoryName { get; set; }
        public string? ProductName { get; set; }
        public decimal? Price { get; set; }
        public int? StockQuantity { get; set; }
        public IFormFile? ImageFile { get; set; }
        public string? Ingredients { get; set; }
        public string? UsageInstructions { get; set; }
        public string? Benefits { get; set; }
    }
}