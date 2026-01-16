using System;

namespace AtmosAQ.Application.Shared
{
    public class MeasurementDto
    {
        public int LocationId { get; set; }
        
        public string Location { get; set; }
        
        public string Parameter { get; set; }
        
        public int Value { get; set; }
        
        public DateTime LastUpdated { get; set; } 
        
        public string Unit { get; set; }
        
        public string Country { get; set; }
        
        public string City { get; set; }
        
        public bool IsMobile { get; set; }
        
        public bool IsAnalysis { get; set; }
        
        public string Entity { get; set; }
        
        public string SensorType { get; set; }
    }
}