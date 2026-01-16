package hu.elte.minineptun.controllers;

import hu.elte.minineptun.entities.Student;
import hu.elte.minineptun.entities.Timetable;
import hu.elte.minineptun.repositories.StudentRepository;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author Lehel T420
 */

@RestController
@RequestMapping("/api/students")
public class StudentController {
    @Autowired
    private StudentRepository studentRepository;
    
    @GetMapping("")
    public ResponseEntity<Iterable<Student>> getAll() {
        return ResponseEntity.ok(studentRepository.findAll());
    }
    
    @PostMapping("")
    public ResponseEntity<Student> post (@RequestBody Student student) {
        student.setId(null);
        return ResponseEntity.ok(studentRepository.save(student));
    }
    
    @GetMapping("/{id}")
    public ResponseEntity<Student> get (@PathVariable Integer id) {
        Optional<Student> oStudent = studentRepository.findById(id);
        if (!oStudent.isPresent()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(oStudent.get());
    }
    
    @PutMapping("/{id}")
    public ResponseEntity<Student> put (@PathVariable Integer id, @RequestBody Student student) {
        Optional<Student> oStudent = studentRepository.findById(id);
        if (!oStudent.isPresent()) {
            return ResponseEntity.notFound().build();
        }
        student.setId(id);
        return ResponseEntity.ok(studentRepository.save(student));
    }
    
    @DeleteMapping("/{id}")
    public ResponseEntity delete (@PathVariable Integer id) {
        Optional<Student> oStudent = studentRepository.findById(id);
        if (!oStudent.isPresent()) {
            return ResponseEntity.notFound().build();
        }
        studentRepository.delete(oStudent.get());
        return ResponseEntity.ok().build();
    }
    
    @GetMapping("/{id}/timetable")
    public ResponseEntity<Timetable> getTimetable(@PathVariable Integer id) {
        Optional<Student> oStudent = studentRepository.findById(id);
        if (!oStudent.isPresent()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(oStudent.get().getTimetable());
    }
}
