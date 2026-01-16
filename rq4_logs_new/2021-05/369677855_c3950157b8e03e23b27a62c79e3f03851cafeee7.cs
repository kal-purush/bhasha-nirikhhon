using System;

namespace AtmosAQ.Application.AveragesData.Queries
{
    public class AverageDto
    {
        public int Id { get; set; }
        
        public string Name { get; set; }

        public string Subtitle { get; set; }

        public string Unit { get; set; }
        
        public DateTime Day { get; set; }
        
        public DateTime Month { get; set; }
        
        public DateTime Year { get; set; }
        
        public float Average { get; set; }
        
        public string Parameter { get; set; }
        
        public string DisplayName { get; set; }
    }
}