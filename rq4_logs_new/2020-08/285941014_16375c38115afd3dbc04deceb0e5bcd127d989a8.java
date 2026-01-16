package lk.ac.cmb.ucsc.scs3203.miniproject.backend.controller;

import lk.ac.cmb.ucsc.scs3203.miniproject.backend.models.ClassEntity;
import lk.ac.cmb.ucsc.scs3203.miniproject.backend.models.ClassEntityData;
import lk.ac.cmb.ucsc.scs3203.miniproject.backend.models.Student;
import lk.ac.cmb.ucsc.scs3203.miniproject.backend.service.ClassEntityService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalTime;
import java.util.List;
import java.util.Set;

@RestController
@RequestMapping("/api/v1/class")
public class ClassEntityController {

    ClassEntityService classEntityService;
    public ClassEntityController(ClassEntityService classEntityService) {
        this.classEntityService = classEntityService;
    }

    @PostMapping
    public ResponseEntity createClass(@RequestBody ClassEntityData classEntityData){
        ClassEntity classEntity =classEntityService.addNewClassEntity(classEntityData);
        if (classEntity != null)
            return new ResponseEntity(HttpStatus.OK);
        else
            return new ResponseEntity(HttpStatus.BAD_REQUEST);
    }

    @PatchMapping("/{id}")
    public boolean updateStudent(@RequestBody ClassEntityData classEntityData, @PathVariable int id){
        return classEntityService.updateClassEntity(id,classEntityData);
    }

    @DeleteMapping("/{id}")
    public void deleteStudent(@PathVariable int id){
        classEntityService.deleteClassEntity(id);
    }

    @GetMapping
    public Set<ClassEntity> getAllClasses(){
        return classEntityService.getAllClasses();
    }

    @GetMapping("/{id}")
    public ClassEntity getClassById(@PathVariable int id){
        return classEntityService.findClassByID(id);
    }

    @GetMapping("/teacher/{teacher_id}")
    public Set<ClassEntity> getClassesForTeacher(@PathVariable int teacher_id){return classEntityService.findClassByTeacher(teacher_id);}

    @GetMapping("/student/{student_id}")
    public Set<ClassEntity> getClassesForStudent(@PathVariable int student_id){return classEntityService.findClassByStudent(student_id);}

    @GetMapping("/day/{day}")
    public Set<ClassEntity> getClassesForDay(@PathVariable String day){return classEntityService.findClassesByDay(day);}

    @GetMapping("/hall/{hall}")
    public Set<ClassEntity> getClassesForHall(@PathVariable String hall){return classEntityService.findClassByHall(hall);}

    @GetMapping("/time/{time}")
    public Set<ClassEntity> getClassesForTime(@PathVariable LocalTime time){return classEntityService.findClassesByTime(time);}

    @GetMapping("/time_day/{time}/{day}")
    public Set<ClassEntity> getClassesForTimeAndDay(@PathVariable LocalTime time,@PathVariable String day){
        return classEntityService.findClassByDayAndTime(day,time);
    }

    @GetMapping("/time_day_hall/{time}/{day}/{hall}")
    public ClassEntity getClassesForTimeAndDayAndHall(@PathVariable LocalTime time,@PathVariable String day,@PathVariable String hall){
        return classEntityService.findClassByHallAndDayAndTime(hall, day, time);
    }

}