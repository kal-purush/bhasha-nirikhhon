/**
 * A program to carry on conversations with a human user.
 * This is the initial version that:  
 * <ul><li>
 *       Uses indexOf to find strings
 * </li><li>
 * 		    Handles responding to simple words and phrases 
 * </li></ul>
 * This version uses a nested if to handle default responses.
 * @author Laurie White
 * @version April 2012
 */
public class Magpie2
{
	/**
	 * Get a default greeting 	
	 * @return a greeting
	 */
	public String getGreeting()
	{
		return "Hello, let's talk.";
	}
	
	/**
	 * Gives a response to a user statement
	 * 
	 * @param statement
	 *            the user statement
	 * @return a response based on the rules given
	 */
	public String getResponse(String statement)
	{
		String response = "";
		if (statement.indexOf("no") >= 0)
		{
			response = "Why so negative?";
		}
		else if (statement.indexOf("mother") >= 0
				|| statement.indexOf("father") >= 0
				|| statement.indexOf("sister") >= 0
				|| statement.indexOf("brother") >= 0)
		{
			response = "Tell me more about your family.";
		}
		else if (statement.indexOf("Cat")>= 0
			|| statement.indexOf("Dog") >= 0
			|| statement.indexOf("Pets")>= 0){
			
			response = "Tell me more about your pets.";
			
		}else if (statement.indexOf("Dencker")>= 0
				){
				
				response = "Mr. *Subject name here* Sounds like a good *Profession name here*";
				
		}else if (statement.indexOf("")>= 0){
			response = "TALK!!!!!";}
		else{
			response = getRandomResponse();
		}
		return response;
	}

	private String getRandomResponse()
	{
		final int NUMBER_OF_RESPONSES = 4;
		double r = Math.random();
		int whichResponse = (int)(r * NUMBER_OF_RESPONSES);
		String response = "";
		
		if (whichResponse == 0)
		{
			response = "Interesting, tell me more.";
		}
		else if (whichResponse == 1)
		{
			response = "Hmmm.";
		}
		else if (whichResponse == 2)
		{
			response = "Do you really think so?";
		}
		else if (whichResponse == 3)
		{
			response = "You don't say.";
		}
		else if (whichResponse == 4){
			response = "Did you know that 2 people die every second on earth?";
		}else if (whichResponse == 5){
			response = "Chinpokomon! Gotta buy em all!";
		}

		return response;
	}
	
	
	
	private int findKeyword(String statement, String goal, int startPos)
	{
	 String phrase = statement.trim();
	 int psn = phrase.toLowerCase().indexOf(goal.toLowerCase(), startPos);
	 while (psn >= 0)
	 {
	 String before = " ", after = " ";
	 if (psn > 0)
	 {
	 before = phrase.substring (psn - 1, psn).toLowerCase();
	 }
	 if (psn + goal.length() < phrase.length())
	 {
	 after = phrase.substring(psn + goal.length(),
	 psn + goal.length() + 1).toLowerCase();
	 }
	 /* determine the values of psn, before, and after at this point in the method. */
	 if (((before.compareTo ("a") < 0 ) || (before.compareTo("z") > 0))
	 &&
	 ((after.compareTo ("a") < 0 ) || (after.compareTo("z") > 0)))
	 {
	 return psn;
	 }
	 psn = phrase.indexOf(goal.toLowerCase(), psn + 1);
	 }
	 return -1; }
}