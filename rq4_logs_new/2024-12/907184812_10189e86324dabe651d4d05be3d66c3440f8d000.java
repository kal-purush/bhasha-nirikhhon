package tabom.myhands.domain.user.entity;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.persistence.*;
import lombok.*;
import tabom.myhands.domain.user.dto.UserRequest;

@Entity
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Table(name = "user")
public class User {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "department_id", nullable = false)
    private Integer departmentId;

    @Column(nullable = false)
    private String name;

    @Column(unique = true, nullable = false, columnDefinition = "varchar(30)")
    private String email;

    @Column(nullable = false, columnDefinition = "varchar(20)")
    private String password;

    private String photo;

    @Column(name = "dayoff_cnt")
    private Float dayoffCnt;

    @Column(name = "employee_num", unique = true, nullable = false)
    private Integer employeeNum;

    @Column(name = "joined_at", nullable = false, updatable = false)
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime joinedAt;

    @Column(name = "created_at", nullable = false)
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    public static User addUser(UserRequest.Join request) {
        return User.builder()
                .departmentId(request.getDepartmentId())
                .name(request.getName())
                .email(request.getEmail())
                .password(request.getPassword())
                .photo(request.getPhoto())
                .dayoffCnt(request.getDayoffCnt())
                .employeeNum(request.getEmployeeNum())
                .joinedAt(request.getJoinedAt())
                .createdAt(LocalDateTime.now())
                .build();
    }
}