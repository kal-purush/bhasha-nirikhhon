
public class Contact {
    public String name;
    public String phoneNumber;
    public String email;



    public Contact(String name, String phoneNumber, String email) {
        this.name = name;
        this.phoneNumber = phoneNumber;
        this.email = email;

    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(this.name);
        sb.append(" ");
        sb.append("<" + this.phoneNumber + ">");
        sb.append(" ");
        sb.append("(" + this.email + ")");
        return sb.toString();
    }
}