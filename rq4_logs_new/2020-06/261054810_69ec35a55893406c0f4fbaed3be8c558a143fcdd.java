package br.com.codenation.v1.errorManager.dto;

import br.com.codenation.v1.errorManager.enums.Profile;

import java.util.Set;
import java.util.stream.Collectors;

public class UserInfoDTO {

    private Long id;
    private String username;
    private String createdAt;
    private String updatedAt;
    private boolean isActive;
    private Set<Profile> profiles;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    public void setProfiles(Set<Profile> profiles) {
        this.profiles = profiles;
    }

    public Set<Profile> getProfile() {
        return profiles;
    }
}