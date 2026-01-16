package studentenrollment.StudentinfoApi.Repository;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import org.springframework.stereotype.Service;
import studentenrollment.StudentinfoApi.Model.User;

@Service
public interface UserRepo extends JpaRepository<User, Integer> {

    @Query(value = "SELECT u.id FROM User u WHERE u.student_id = :student_id", nativeQuery = true)
    Integer findUserById(@Param("student_id") Integer studentId);
    User findByStudentId(Integer studentId);
}
