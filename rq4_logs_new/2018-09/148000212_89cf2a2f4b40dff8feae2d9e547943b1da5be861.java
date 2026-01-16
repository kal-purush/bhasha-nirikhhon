package mapper;

import org.apache.ibatis.annotations.Param;
import pojo.Role;

import java.util.List;

public interface SexMapper {
    Role getRole(@Param("id") int id);
    int updateRole(Role role);
    int insertRole(Role role);
    List<Role> findRoles(@Param("roleName") String roleName,@Param("note") String note);
}