package ca.sheridancollege.fourothreeindustries.domain;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DateEvents {

	private int Date;
	private List<Event> events;
}