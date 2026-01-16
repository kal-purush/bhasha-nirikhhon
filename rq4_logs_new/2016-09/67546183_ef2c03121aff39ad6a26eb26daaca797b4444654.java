/*
 * Araceli Gopar
 * QuestionsAnswers.java
 * Description: Write a class that sets up a map of questions and answers
 * 				and test it with a unit test class. 				
 */
import java.util.HashMap;
import java.util.Random;


public class QuestionsAnswers {
	/**
	* Maps question to answer.
	*/
	HashMap<String, String> questions = new HashMap<String, String>();
	
void put(String question, String answer){
	/**
	* Queries if question maps to answer.
	*  */
	
	
	questions.put(question, answer);
	
}
boolean testAnswer(String question, String answer){
	/**
	* Gives out a random question from the key set. */
	
	for (String key : questions.keySet()) {
	    if (questions.get(key).contains(answer)) {
	        return true;
	    }
	}
	return false;
	
	}

String getRandomQuestion(){
	

	Object[] randQuestion = questions.keySet().toArray();
	String question = "Empty";
	
	if(questions.size() > 0){
    Object key = randQuestion[new Random().nextInt(randQuestion.length)];
   // System.out.println(questions.get(key));
     question = questions.get(key);
     //System.out.println(question);
     return question;
	}
	return "Empty";
	
}
}