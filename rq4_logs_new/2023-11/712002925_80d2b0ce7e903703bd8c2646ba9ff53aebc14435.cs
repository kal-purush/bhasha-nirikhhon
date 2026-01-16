namespace FalknerCountyJuvenileCourt.Models
{
    public class CrimeResult
    {
        public int CCID { get; set; }
        public int Juvenile_ID { get; set; }
        public int Crime_ID { get; set; }
        public int School_ID { get; set; }
        public int IntakeDescision_ID { get; set; }
        public int Pros_Filing_Desc_ID { get; set; }
        public DateTime Date {  get; set; }
        public bool Drug_Court { get; set; }
        public int Drug_Offense { get; set; }

        public Juvenile Juvenile { get; set; }
        public Crime Crime { get; set; }
        public Schools School { get; set; }
        public ProsFilingDesc ProsFilingDesc { get; set; }
        public IntakeDesc IntakeDesc { get; set; }
        
        public ICollection<CrimeResult> CrimeResults { get; set; }
    }
}