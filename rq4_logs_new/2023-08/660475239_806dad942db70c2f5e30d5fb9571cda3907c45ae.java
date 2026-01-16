package day19constructorsaccesmodifiersstatic;

public class TeacherRunner {
    public static void main(String[] args) {

        Teacher myTeacher = new Teacher();
        System.out.println(myTeacher.teacherName);
        myTeacher.teacherName = "Ali"; // teacherName accecible
        myTeacher.phone = 765765;     // phone accecible
        myTeacher.salary = 987987;    // salary accecible
                                      // ssn is not accecible
        System.out.println(myTeacher.teacherName);

    }
}