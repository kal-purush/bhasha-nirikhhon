import java.util.ArrayList;

public class Email {
  private static ArrayList<Email> instances = new ArrayList<Email>();
  // private static ArrayList<Contact> mContacts;

  private String mAddress;
  private String mType;
  private int mId;

  public Phone(String address, String type) {
    mAddress = areaCode;
    mType = type;
    instances.add(this);
    mId = instances.size();
    mContacts = new ArrayList<Contact>();
  }

  public String getAddress() {
    return mAddress;
  }

  public int getId() {
    return mId;
  }

  public String getType() {
    return mType;
  }
  // 
  // public ArrayList<Contact> getContacts() {
  //   return mContacts;
  // }
  //
  // public void addTask(Contact contact) {
  //   mContacts.add(contact);
  // }

  public static ArrayList<Email> all() {
    return instances;
  }

  public static Phone find(int id) {
    try {
      return instances.get(id - 1);
    } catch (IndexOutOfBoundsException e){
      return null;
    }
  }

  public static void clear() {
    instances.clear();
  }
}