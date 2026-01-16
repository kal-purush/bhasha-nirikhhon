package com.dupy.F1;


import com.dupy.F1.dao.CarRepository;
import com.dupy.F1.dao.CourseRepository;
import com.dupy.F1.dao.PilotRepository;
import com.dupy.F1.model.Car;
import com.dupy.F1.model.Course;
import com.dupy.F1.model.Pilot;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class Ajout implements CommandLineRunner {
    @Autowired
    private PilotRepository pilotRepository;
    @Autowired
    private CarRepository carRepository;
    @Autowired
    private CourseRepository courseRepository;

    @Override
    public void run(String... args) throws Exception {
        for (int i = 1; i < 11; i++) {
            Pilot data = new Pilot();
            data.setName("Pilot " + i);
            pilotRepository.save(data);
        }
        for (int i = 1; i < 3; i++) {
            Course data = new Course();
            data.setName("Course "+i);
            data.setPays("Pays "+i);
            courseRepository.save(data);
        }
        for (int i = 1; i < 11; i++) {
            Car data = new Car();
            data.setName("Car " + i);
            data.setSpeed((int) (Math.random() * 100));
            data.setPilot(pilotRepository.findById(i).get());
            data.setCourse(courseRepository.findById(1).get());
            carRepository.save(data);
        }
    }


}