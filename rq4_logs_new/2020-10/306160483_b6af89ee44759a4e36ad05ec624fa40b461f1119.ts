import { isEqual } from 'date-fns';
import Appointment from '../model/Appointment';

class AppointmentRepository {
  private appointments: Appointment[];

  constructor() {
    this.appointments = [];
  }

  findByDate(date: Date): Appointment | null {
    const findAppointmentInSameDate = this.appointments.find(appointment =>
      isEqual(date, appointment.date),
    );

    return findAppointmentInSameDate || null;
  }

  create(provider: string, date: Date): Appointment {
    const appointment = new Appointment(provider, date);

    this.appointments.push(appointment);
    return appointment;
  }
}

export default AppointmentRepository;