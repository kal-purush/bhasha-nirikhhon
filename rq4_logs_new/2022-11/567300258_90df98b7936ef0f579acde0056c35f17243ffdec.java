package hu.webuni.student.andro;

import java.time.LocalDate;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor
public class FromToLocalDate {

	@XmlJavaTypeAdapter(value = LocalDateAdapter.class,type = LocalDate.class)
	LocalDate from;
	
	@XmlJavaTypeAdapter(value = LocalDateAdapter.class,type = LocalDate.class)
	LocalDate to;
	
}