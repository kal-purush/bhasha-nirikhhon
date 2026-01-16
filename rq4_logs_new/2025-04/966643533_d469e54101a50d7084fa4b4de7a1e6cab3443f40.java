package org.spiceboys.Travel.Diary.dto;

public class PublicUserDTO extends UserDTO {
    private Long userId;
    private String username;
    private String bio;
    private String profilePicUrl;

    public PublicUserDTO(Long userId,
                         String username,
                         String bio,
                         String profilePicUrl,
                         Boolean isPrivate) {
        this.userId = userId;
        this.username = username;
        this.bio = bio;
        this.profilePicUrl = profilePicUrl;
        this.isPrivate = isPrivate;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getBio() {
        return bio;
    }

    public void setBio(String bio) {
        this.bio = bio;
    }

    public String getProfilePicUrl() {
        return profilePicUrl;
    }

    public void setProfilePicUrl(String profilePicUrl) {
        this.profilePicUrl = profilePicUrl;
    }
}
