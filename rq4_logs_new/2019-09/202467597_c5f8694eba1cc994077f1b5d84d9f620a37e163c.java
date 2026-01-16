package Beans;

public class Comments {
    private Integer id;
    private String username;
    private String storename;
    private Integer score;
    private String detail;

    public Comments(){

    }
    public Comments(Integer id, String username, String storename,Integer score, String detail){
        this.id =id;
        this.username= username;
        this.storename = storename;
        this.score =score;
        this.detail = detail;
    }


    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }


    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getStorename() {
        return storename;
    }

    public void setStorename(String storename) {
        this.storename = storename;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }
}