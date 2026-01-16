package shako.schoolmanagement.dtomapper;

import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import shako.schoolmanagement.dto.CourseDto;
import shako.schoolmanagement.entity.Course;

@Configuration
public class CourseMapper {

    @Autowired
    private ModelMapper modelMapper;

    public CourseDto courseToCourseDto(Course course){
        return modelMapper.map(course, CourseDto.class);
    }

    public Course courseDtoToCourse(CourseDto courseDto){
        return modelMapper.map(courseDto, Course.class);
    }
}