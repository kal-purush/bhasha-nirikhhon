import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

class AppointmentScheduler {
    public LocalDateTime schedule(String appointmentDateDescription) {
        DateTimeFormatter parser = DateTimeFormatter.ofPattern("M/d/yyyy HH:mm:ss");
        return  LocalDateTime.parse(appointmentDateDescription, parser);
    }

    public boolean hasPassed(LocalDateTime appointmentDate) {
        LocalDateTime currenTime = LocalDateTime.now();
        return appointmentDate.isBefore(currenTime);
    }

    public boolean isAfternoonAppointment(LocalDateTime appointmentDate) {
        //return (appointmentDate.getHour()>= 12 && appointmentDate.getHour() < 18);
        if(appointmentDate.getHour()>= 12 && appointmentDate.getHour() < 18){
            return true ;
        }
        return false ;
    }

    public String getDescription(LocalDateTime appointmentDate) {
        String day = appointmentDate.getDayOfWeek().toString();
        String firstLed = String.valueOf(day.charAt(0));
        String restWd = day.substring(1).toLowerCase();
        String month = appointmentDate.getMonth().toString().toLowerCase();
        String firstLe = String.valueOf(month.charAt(0));
        String restW = month.substring(1).toLowerCase();
        int daymon = appointmentDate.getDayOfMonth();
        int year = appointmentDate.getYear();
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("h:mm a");
        String formattedTime = appointmentDate.format(timeFormatter);
        formattedTime = formattedTime.replace("p. m.", "PM");
        //"You have an appointment on Friday, March 29, 2019, at 3:00 PM."
        return "You have an appointment on " + firstLed+restWd + ", "+ firstLe.toUpperCase()+restW +" "+daymon+", "+year
                +", at "+formattedTime+".";
    }

    public LocalDate getAnniversaryDate() {
        int currenTime = LocalDateTime.now().getYear();
        return LocalDate.of(currenTime,9,15);
    }
}