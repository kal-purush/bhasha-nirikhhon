namespace Domain;
public class AppointmentForm
{
    public DateTime Start { get; set; }
    public DateTime End { get; set; }
    public int PatientID { get; set; }
    public int DoctorID { get; set; } = -0xdeadb0b; // magic number == doctor not selected
    public string Specialization { get; set; }

    public AppointmentForm(DateTime start, DateTime end, int patientID, int doctorID, string specialization)
    {
        Start = start;
        End = end;
        PatientID = patientID;
        DoctorID = doctorID;
        Specialization = specialization;
    }
}