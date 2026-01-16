public class GetterSetter {
    public static void main(String[] args) {
        // 클래스 객체 생성하기
        Stage stage = new Stage();

        // 이름 셋팅
        stage.setName("톰 행크스");
        System.out.println("배우의 이름은 "+stage.getName());
    }
}

class Stage{
    private int num;
    private boolean myb;
    private double numnum;
    private String name;
    private String addr;
    private int password;
    private String mytext;

    public int getNum() {
        return this.num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public boolean isMyb() {
        return this.myb;
    }

    public void setMyb(boolean myb) {
        this.myb = myb;
    }

    public double getNumnum() {
        return this.numnum;
    }

    public void setNumnum(double numnum) {
        this.numnum = numnum;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddr() {
        return this.addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public int getPassword() {
        return this.password;
    }

    public void setPassword(int password) {
        this.password = password;
    }

    public String getMytext() {
        return this.mytext;
    }

    public void setMytext(String mytext) {
        this.mytext = mytext;
    }

}