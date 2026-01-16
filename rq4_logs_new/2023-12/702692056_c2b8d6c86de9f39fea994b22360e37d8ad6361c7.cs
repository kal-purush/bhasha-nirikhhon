namespace MentalHospital.API.ViewModels;

public class SessionViewModel
{
	public Guid Id { get; set; }

	public Guid? DoctorId { get; set; }

	public DoctorViewModel? Doctor { get; set; }

	public Guid? PatientId { get; set; }

	public PatientViewModel? Patient { get; set; }

	public DateTime SessionDateTime { get; set; }

	public string? Result { get; set; }
}