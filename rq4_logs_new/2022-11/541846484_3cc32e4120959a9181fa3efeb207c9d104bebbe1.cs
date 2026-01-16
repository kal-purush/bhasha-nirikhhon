using Domain;

namespace Repository;

public class AppointmentRepositoryImpl : IAppointmentRepository
{
    private ServiceContext _context;

    public AppointmentRepositoryImpl(ServiceContext context)
    {
      _context = context;
    }
    public Appointment? CreateAppointment(AppointmentForm form)
    {
      int newID = _context.Appointments.Count() == 0 ? 1 : _context.Appointments.Last().ID;
      var appointment = new AppointmentModel
      {
          ID = newID,
          Start = form.Start,
          End = form.End,
          PatientID = form.PatientID,
          DoctorID = form.DoctorID
      };
      _context.Appointments.Add(appointment);

      var check = _context.Appointments.FirstOrDefault(ap => ap.DoctorID == form.DoctorID &&
          ap.PatientID == form.PatientID && ap.Start == form.Start && ap.End == form.End);

      if (check is null) return null;

      return new Appointment(
          check.Start,
          check.End,
          check.PatientID,
          check.DoctorID
      );
    }
    public bool AppointmentExists(AppointmentForm form)
    {
      var appointment = _context.Appointments.FirstOrDefault(ap => ap.DoctorID == form.DoctorID &&
          ap.PatientID == form.PatientID && ap.Start == form.Start && ap.End == form.End);
      
      if (appointment is null) return false;

      return true;
    }

    public List<DateTime>? GetAllDates(string specialization)
    {
      var appointments = _context.Appointments.Where(ap => ap.Specialization.Name == specialization);

      if (appointments is null) return null;

      List<DateTime> dates = new List<DateTime>();
      foreach(var appointment in appointments)
      {
        dates.Append(appointment.Start.Date);
      }

      return dates;
    }
}