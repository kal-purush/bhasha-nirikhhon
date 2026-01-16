
package acme.forms;

import java.util.Map;

import acme.entitites.lecture.LectureType;
import acme.framework.data.AbstractForm;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LecturerDashboard extends AbstractForm {

	// Serialisation identifier -----------------------------------------------

	protected static final long	serialVersionUID	= 1L;

	// Attributes -------------------------------------------------------------

	Map<LectureType, Integer>	countOfLecturesByType;

	Map<String, Double>			statisticsLearningTimeLectures;

	Map<String, Double>			statisticsLearningTimeCourses;

	// Relationships ----------------------------------------------------------

}