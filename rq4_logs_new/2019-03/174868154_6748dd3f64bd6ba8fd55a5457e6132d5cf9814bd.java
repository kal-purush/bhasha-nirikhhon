package application;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.Period;
import java.util.Calendar;
import java.util.Date;

public class Calendario {

	public static void main(String[] args) {
		
		
		
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		
		Date d = Date.from(Instant.parse("2018-06-15T15:42:07Z")); //Formato ISO 8601
		
		System.out.println(sdf.format(d));
		
		
		Calendar cal = Calendar.getInstance();
		cal.setTime(d); //Data instanciada
		cal.add(Calendar.HOUR_OF_DAY, 4); // Adcionando 4 horas
		d = cal.getTime();
		
		System.out.println(sdf.format(d));
		
		int minutes = cal.get(Calendar.MINUTE); // Pegando a quantidade de minutos do dia
		System.out.println(minutes);
		
		int horas = cal.get(Calendar.HOUR_OF_DAY);
		System.out.println(horas);
		
		int month = 1 + cal.get(Calendar.MONTH);
		System.out.println(month);
		
		Instant agora = Instant.now();
		System.out.println(agora);
		
		LocalDate hoje = LocalDate.now();
		System.out.println(hoje);
		
		LocalDate homemNoEspaco  = LocalDate.of(1961, Month.APRIL, 12);
		LocalDate homemNaLua = LocalDate.of(1969, Month.MAY, 25);
		
		Period periodo = Period.between(homemNoEspaco, homemNaLua);
		
		System.out.printf("%s anos, %s mes e %s dias",
				periodo.getYears(), periodo.getMonths(), periodo.getDays());
		
		
	}

}