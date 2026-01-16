package hu.uni.miskolc.iit.service.impl;

import hu.uni.miskolc.iit.StudentServiceDao;
import hu.uni.miskolc.iit.model.Course;
import hu.uni.miskolc.iit.service.StudentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class StudentServiceImpl implements StudentService {

    @Autowired
    StudentServiceDao studentServiceDao;

    /**
     * Service layer method to get all listed courses by sending the request to the DAO layer
     *
     * @return
     */
    @Override
    public List<Course> getAllCourses() {
        return studentServiceDao.findAllCourses();
    }
}