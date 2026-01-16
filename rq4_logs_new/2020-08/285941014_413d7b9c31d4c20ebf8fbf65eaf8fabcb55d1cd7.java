package lk.ac.cmb.ucsc.scs3203.miniproject.backend;

import lk.ac.cmb.ucsc.scs3203.miniproject.backend.models.Student;
import lk.ac.cmb.ucsc.scs3203.miniproject.backend.repository.StudentRepository;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.annotation.Testable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;

@Testable
@SpringBootTest
public class StudentDummyDataTest {

    @Autowired
    StudentRepository repository;

    @Test
    public void newStudentDummyData(){
        List<Student> list=new ArrayList();
        list.add(new Student("Missaka","Iddamalgoda","New Town","Ratnapura","0771234567","midda@midda.com"));
        list.add(new Student("Lahiru","Pathum","","Galle","0721234567","lariya@lariya.com"));
        list.add(new Student("Sujith","Perera","Hirana","Panadura","0711234567","suji@kota.com"));
        list.add(new Student("Abhimani","Subasinghe","Udunuwara","Kandy","0751234567","abhi@sleepiness.com"));

        repository.saveAll(list);
    }

}