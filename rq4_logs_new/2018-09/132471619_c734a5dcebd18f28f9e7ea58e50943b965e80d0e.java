package br.com.outtec.utils;

import java.util.Calendar;
import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.springframework.stereotype.Component;

@Component
public class TimesheetUtils {
	
	public String formataHora(Integer horas, Integer minutos){
		String horaFormatada = "";
		if (horas< 10){
			horaFormatada = "0"+horas+":";
		}
		if (minutos< 10){
			horaFormatada = horaFormatada +"0"+minutos;
		}else {
			horaFormatada = horaFormatada + minutos;
		}
		return horaFormatada;
	}
	
	
    public void calculaHora(Date start, Date end) {

	Calendar cal = Calendar.getInstance();
	cal.setTime(start);

	Integer anoCorrente = cal.get(Calendar.YEAR);
	Integer mesCorrente = cal.get(Calendar.MONTH)+1;
	Integer diaCorrente = cal.get(Calendar.DAY_OF_MONTH);
	
	DateTime inicioDia = new DateTime(anoCorrente, mesCorrente, diaCorrente, 0, 0);
	DateTime inicioUtil = new DateTime(anoCorrente, mesCorrente, diaCorrente, 6, 0);
	DateTime inicioNoite = new DateTime(anoCorrente,mesCorrente, diaCorrente, 21, 0);
	DateTime finalNoite = new DateTime(anoCorrente, mesCorrente,diaCorrente, 23, 59);
	
	DateTime startDT = new DateTime(start);
	DateTime endDT = new DateTime(end);
	
	Interval iManha = new Interval(inicioDia.getMillis(), inicioUtil.getMillis());
	Interval iUtil = new Interval(inicioUtil.getMillis(), inicioNoite.getMillis());
	Interval iNoite = new Interval(inicioNoite.getMillis(), finalNoite.getMillis());
	
	Interval i = new Interval(startDT.getMillis(), endDT.getMillis());
	
	if (iManha.overlaps(i)) {
	    Interval iExtraManha = iManha.overlap(i);
	    System.out.println("#Horas Extras pela Manhã => " + formataHora(iExtraManha.toPeriod().getHours(), iExtraManha.toPeriod().getMinutes()));	    
	}
	if (iUtil.overlaps(i)) {
	    Interval iHorasUteis = iUtil.overlap(i);
	    
	    System.out.println("#Horas Normais => " + formataHora(iHorasUteis.toPeriod().getHours(),iHorasUteis.toPeriod().getMinutes()));	    
	}
	if (iNoite.overlaps(i)) {
	    Interval iExtraNoite = iNoite.overlap(i);
	    System.out.println("#Horas Extras pela Noite => " + formataHora(iExtraNoite.toPeriod().getHours(),iExtraNoite.toPeriod().getMinutes()));	    
	}
	
    }
}