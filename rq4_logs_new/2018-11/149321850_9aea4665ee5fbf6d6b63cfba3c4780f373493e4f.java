package hu.uni.miskolc.iit;

public class Course {


    private String code;
    private String name;
    private String description;
    private Teacher teacher;

    public Course(String code, String name, String desciption, Teacher teacher){
        this.code = code;
        this.name = name;
        this.description = desciption;
        this.teacher = teacher;
    }

    public String getCode() {
        return code;
    }
    public void setCode(String code) {
        this.code = code;
    }
    public String getName() {
        return name;
    }
    public void setName(String courseName) {
        this.name = courseName;
    }
    public String getDescription() {
        return description;
    }
    public void setDescription(String description) {
        this.description = description;
    }
    public Teacher getTeacher() {
        return teacher;
    }
    public void setTeacher(Teacher teacher) {
        this.teacher = teacher;
    }

    public void checkCode(String code){
        if(code == null){
            throw new IllegalArgumentException("The course code can't be null");
        }
        else if(code.length()!=6){
            throw new IllegalArgumentException("The course code must be 6 characters long");
        }
    }

    public void checkName(String name){
        if(name == null){
            throw new IllegalArgumentException("The name can't be null");
        }
    }

    public void checkDescription(String desc){
        if(desc == null){
            throw new IllegalArgumentException("The course description can't be null");
        }
    }

}