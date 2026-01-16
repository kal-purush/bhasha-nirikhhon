import org.bson.codecs.pojo.annotations.BsonProperty;

public class UserDBModel {
    private int longitude;
    @BsonProperty(value = "longitude")

    private int latitude;
    @BsonProperty(value = "latitude")

    private String email;
    @BsonProperty(value = "email")

    private String name;
    @BsonProperty(value = "name")

    private String password;
    @BsonProperty(value = "password")


    private String uid;
    public UserDBModel(int[] coords, String userEmail, String userPassword, String name, String uid){
        this.password = userPassword;
        this.latitude = coords[0];
        this.longitude = coords[1];
        this.email = userEmail;
        this.name = name;
        this.uid = uid;
    }

}