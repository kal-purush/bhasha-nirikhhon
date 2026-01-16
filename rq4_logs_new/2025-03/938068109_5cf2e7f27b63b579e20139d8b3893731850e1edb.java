public class Admin extends User{
    int accesslevel;

    public Admin(String name,String email,int accesslevel){
        super(name, email);
        this.accesslevel=accesslevel;
    }
    public void display(){
        super.display();
        System.out.println(accesslevel);
    }
}