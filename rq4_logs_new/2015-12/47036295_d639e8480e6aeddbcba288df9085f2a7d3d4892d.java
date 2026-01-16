import java.util.Calendar;
import java.util.List;
import java.util.Set;

/**
* A class to manage your contacts and meetings.
*
*/


public interface ContactManager {
	
	/**
	* Create a new contact with the specified name and notes.
	* 
	* @param name the name of the contact.
	* @param notes notes to be added about the contact.
	* @return the ID for the new contact
	* @throws IllegalArgument if the name or the notes are empty strings.
	* @throws NullPointerException if the name or the notes are nulll.
	*
	*/
	int addNewContact (String name, String notes);

	//int addFutureMeeting (Set<Contact> contacts, Calendar date);
	
	//PastMeeting getPastMeeting(int id);
	
	//FutureMeeting getFutureMeeting(int id);
	
	//Meeting getMeeting(int id);
	
	//List<Meeting> getFutureMeetingList(Contact contact);
	
	//List<Meeting> getFutureMeetingList(Calendar date);
	
	//List<Meeting> getPastMeetingList(Contact contact);
	
	//void addNewPastMeeting(Set<Contact> contacts, Calendar date, String text);
	
	//void addMeetingNotes(int id, String txt);
	
	//void addNewContact (String name, String notes);
	
	//Set<Contact> getContacts (int ids);
	
	//Set<Contact> getContacts(String name);
	
	//void flush();
		
	
}