namespace CRM_api.DataAccess.Models
{
    public partial class TblMgainSchemeMaster
    {
        public int Id { get; set; }
        public string? Schemename { get; set; }
        public decimal? Interst1 { get; set; }
        public decimal? Interst2 { get; set; }
        public decimal? Interst3 { get; set; }
        public decimal? Interst4 { get; set; }
        public decimal? Interst5 { get; set; }
        public decimal? Interst6 { get; set; }
        public decimal? Interst7 { get; set; }
        public decimal? Interst8 { get; set; }
        public decimal? Interst9 { get; set; }
        public decimal? Interst10 { get; set; }
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public bool? YearlyInterest { get; set; }
        public bool? MonthlyInterest { get; set; }
        public decimal? AdditionalInterest4 { get; set; }
        public decimal? AdditionalInterest5 { get; set; }
        public decimal? AdditionalInterest6 { get; set; }
        public decimal? AdditionalInterest7 { get; set; }
        public decimal? AdditionalInterest8 { get; set; }
        public decimal? AdditionalInterest9 { get; set; }
        public decimal? AdditionalInterest10 { get; set; }
        public bool? IsActive { get; set; }
    }
}