package bit.rigu.homework.lesson8;

import bit.rigu.homework.lesson8.entity.Mathematician;
import bit.rigu.homework.lesson8.entity.University;
import bit.rigu.homework.lesson8.service.MathematicianService;
import bit.rigu.homework.lesson8.service.UniversityService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class TestRunner {
    public static void main(String[] args) {
        ConfigurableApplicationContext context =  SpringApplication.run(TestRunner.class, args);
        MathematicianService mathService = context.getBean(MathematicianService.class);
        UniversityService universityService = context.getBean(UniversityService.class);
        System.out.println("All mathematician: ");
        System.out.println(mathService.findAll());
        System.out.println("**************************************");
        System.out.println("All Universities:");
        System.out.println(universityService.findAll());
        System.out.println("**************************************");
        System.out.println("Mathematician with id = 15 ");
        System.out.println(mathService.findById(15));
        System.out.println("**************************************");
        System.out.println("University with name Université libre de Bruxelles");
        System.out.println(universityService.findByName("Université libre de Bruxelles"));
        System.out.println("**************************************");
        System.out.println("Mathematician who took a Fields Medal in 1990 year");
        System.out.println(mathService.findByYearOfAward(1990));
        System.out.println("**************************************");
        System.out.println("All universities that younger 300 years");
        System.out.println(universityService.findUniversitiesYoungerGivenDataEstablishment(1820));
        System.out.println("***************************************");
        System.out.println("Creating new mathematician with id = 31");
        mathService.create(new Mathematician(
                31, "New Name", 3000, "new nationality", "new area", null
        ));
        System.out.println(mathService.findById(31));
        System.out.println("***************************************");
        System.out.println("Create new university with id = 21");
        universityService.create(new University(
                21, "New university", "new country", 3000
        ));
        System.out.println(universityService.findByName("New university"));
        System.out.println("***************************************");
        System.out.println("Deleting mathematician with nationality new");
        mathService.deletedByNationality("new nationality");
        System.out.println("***************************************");
        System.out.println("Deleting university with id = 21");
        universityService.deleteById(21);
        System.out.println("***************************************");
        System.out.println("Application is over");
    }
}