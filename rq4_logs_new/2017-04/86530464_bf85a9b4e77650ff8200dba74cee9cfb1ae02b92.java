package socialNetworking;
import java.util.ArrayList;
public class User implements Comparable<User>
{
  private String firstName, lastName;
  private int age;
  protected int totalNumMessages, numWallPosts, numPictures;
  public ArrayList<WallPosts> messages;
  protected ArrayList<Pictures> picMessages;
  protected ArrayList<WallPosts> allPosts;
  
  public User(String firstName, String lastName, int age)
  { //Initializes everything the User has
    this.firstName = firstName;
    this.lastName = lastName;
    this.age = age;
    messages = new ArrayList<WallPosts>();
    picMessages = new ArrayList<Pictures>();
    allPosts = new ArrayList<WallPosts>();
    totalNumMessages = 0;
    numWallPosts = 0;
    numPictures = 0;
  }
  
  public String getFirstName()
  { //Simple getters
    return firstName;
  }
  
  public String getLastName()
  {
    return lastName;
  }
  
  public int getAge()
  {
    return age;
  }
  
  public ArrayList<WallPosts> getMessages()
  {
    return messages;
  }
  
  public int getTotalNumMessages()
  {
    return totalNumMessages;
  }
  
  public int getNumWallPosts()
  {
    return numWallPosts;
  }
  
  public int getNumPictures()
  {
    return numPictures;
  }
  
  public void addNumPictures()
  { //for when messages are sent
    numPictures++;
  }
  
  public void addNumWallPosts()
  {
    numWallPosts++;
  }
  
  public void addTotalNumMessages()
  {
    totalNumMessages++;
  }
  
  public void addMessage(WallPosts message)
  { //To add messages to a users ArrayList
    messages.add(message);
  }
  public ArrayList<WallPosts> getWallPosts()
  {
    return messages;
  }
  
  public ArrayList<Pictures> getPicMessages()
  {
    return picMessages;
  }
  
  public ArrayList<WallPosts> getAllPosts()
  {
    return allPosts;
  }

  public String toString()
  { //Correct format for User representation
    String correctFormat = lastName + ", " + firstName + " " + "(" + age + ")";
    return correctFormat;
  }

  public int compareTo(User otherUser)
  { //The sorting algorithm
    return this.toString().compareTo(otherUser.toString());
  }
}