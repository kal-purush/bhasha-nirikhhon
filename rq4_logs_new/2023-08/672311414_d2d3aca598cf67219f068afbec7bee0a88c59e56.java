package ru.scarlet.company.entities;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.Table;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "Course", indexes = {
		@Index(name = "idx_course_oid", columnList = "oid")
})
@Getter
@Setter
//предмет
public class Course {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Integer oid;
	private String courseCode;
	private String courseName;

	private Integer departmentId;

	@ManyToMany(mappedBy = "teachingCourses")
	private List<Professor> taughtByProfessors;


}