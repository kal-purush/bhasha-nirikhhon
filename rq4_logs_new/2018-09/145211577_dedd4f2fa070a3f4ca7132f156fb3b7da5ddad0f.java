package siit.finalProject.VehicleManagement.dto;

import siit.finalProject.VehicleManagement.domain.UserRole;

import java.util.Set;

public class UserRequest {
    private String username;
    private Set<UserRole> userRoles;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Set<UserRole> getUserRoles() {
        return userRoles;
    }

    public void setUserRoles(Set<UserRole> userRoles) {
        this.userRoles = userRoles;
    }
}