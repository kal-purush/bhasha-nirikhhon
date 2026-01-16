package landmarketmavenproto.model;

/**
 * Created by Виктор on 31.05.2017.
 */
public class SellerAuthType {

    private String login;
    private String password;

    public SellerAuthType() {
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String email) {
        this.login = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
