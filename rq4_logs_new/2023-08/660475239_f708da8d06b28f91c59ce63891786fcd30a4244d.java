package day19constructorsaccesmodifiersstatic;

public class Students {

    public String stdName;
    public int age;
    public boolean success;
    public String gender;


    public Students()
    {

    }

    //create a constructor uses all variables
    public Students(String stdName,int age,boolean success,String gender)
    {
        this.stdName=stdName;
        this.age=age;
        this.success=success;
        this.gender=gender;
    }

    //create a constructor uses some variables
    public Students(String stdName,int age,boolean success)
    {
        this.stdName=stdName;
        this.age=age;
        this.success=success;
    }
    // right click mouse Generate>>>Constructor>>>select variables
    public Students(String stdName, int age, String gender) {
        this.stdName = stdName;
        this.age = age;
        this.gender = gender;
    }
}