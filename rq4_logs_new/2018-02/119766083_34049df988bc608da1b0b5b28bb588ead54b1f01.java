package xceder;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: Mac
 * Date: 2018-02-05
 * Time: 上午10:45
 */
public class Person {
    private String name;
    private int age;
    private String school;

    public Person() {
    }

    public Person(String name, int age, String school) {
        this.name = name;
        this.age = age;
        this.school = school;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getSchool() {
        return school;
    }

    public void setSchool(String school) {
        this.school = school;
    }
}