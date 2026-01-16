public class User {
    private int[] homeCooridnates;
    private String email;
    private String name;
    private int uid;
    public User(int[] coords, String userEmail, String name, int uid){
        this.homeCooridnates = coords;
        this.email = userEmail;
        this.name = name;
        this.uid = uid;
    }

    public int[] getHomeCooridnates() {
        return homeCooridnates;
    }

    public void setHomeCooridnates(int[] homeCooridnates) {
        this.homeCooridnates = homeCooridnates;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}