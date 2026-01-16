package uk.ac.bangor.cs.cambria.AcademiGymraeg.questionConstruction;

import org.springframework.stereotype.Component;

import uk.ac.bangor.cs.cambria.AcademiGymraeg.QuestionConstruction;

/**
 * @author cnb22xdk
 */

@Component
public class EnglishQuestionImpl implements QuestionConstruction {
	
	/**
	 * Perform the question construction for an English word
	 */
	@Override
	public String constructQuestion(String a) {
		return "What is the English word for " + a + "?";
	}
	
	/**
	 * Sets the appropriate enumeration value
	 */
	@Override
	public String getQuestionType() {
		return "WELSH_TO_ENGLISH";
	}

}