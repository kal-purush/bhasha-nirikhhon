package Swiggy_Zomato.Swiggy.Details;

public class user {

    private static final String PHONE_NUMBER_REGEX = "^\\+?[1-9]\\d{1,14}$";

    private String Name;
    private String phoneNumber;
    private String Address;

    // Constructor
    public user(String name, String phoneNumber, String address) {
        this.Name = name;
        this.phoneNumber = phoneNumber;
        this.Address = address;
    }

    // Getter and Setter
    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getAddress() {
        return Address;
    }

    public void setAddress(String address) {
        Address = address;
    }

    @Override
    public String toString() {
        return "user [Name=" + Name + ", phoneNumber=" + phoneNumber + ", Address=" + Address + ", getName()="
                + getName() + ", getPhoneNumber()=" + getPhoneNumber() + ", getAddress()=" + getAddress()
                + ", getClass()=" + getClass() + ", hashCode()=" + hashCode() + ", toString()=" + super.toString()
                + "]";
    }

}