package JavaBean;

/**
 * @Author: michael
 * @Date: 16-7-19 上午12:45
 * @Project: S.M.
 * @Package: JavaBean
 */
public class AddressBean {
    private int id;
    private int uid;
    private String address;

    public final short ID = 1;
    public final short UID = 2;
    public final short ADDRESS = 3;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}